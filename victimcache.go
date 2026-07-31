package main

import (
	"fmt"

	"github.com/sarchlab/akita/v4/mem/mem"
	"github.com/sarchlab/akita/v4/mem/vm"
	"github.com/sarchlab/akita/v4/sim"
)

// VCSpec configures a VictimCache instance.
//
// NOTE: BlockSize must match the block size of the cache sitting above the
// victim cache (the L1 in this system uses Log2BlockSize=6 -> 64 bytes), or
// address alignment between the two levels will not line up.
type VCSpec struct {
	Freq      sim.Freq
	Latency   int // extra cycles of lookup latency per top-side transaction
	BlockSize int // must equal L1 block size (64 bytes for this system)
	NumBlocks int // fully-associative -> total capacity = BlockSize*NumBlocks
}

// DefaultVCSpec returns a sensible default: 8 fully-associative blocks,
// 64 bytes each (matching the L1 built in main.go), 2-cycle lookup latency.
func DefaultVCSpec() VCSpec {
	return VCSpec{
		Freq:      1 * sim.GHz,
		Latency:   2,
		BlockSize: 64,
		NumBlocks: 8,
	}
}

// VCBlock is one fully-associative slot in the victim cache.
type VCBlock struct {
	Tag            uint64
	Valid          bool
	Data           []byte
	LastAccessTime uint64
}

// vcTopTransaction tracks an in-flight request received on the top port
// while its artificial lookup latency counts down.
type vcTopTransaction struct {
	IsRead    bool
	IsWrite   bool
	Address   uint64
	ByteSize  uint64
	Data      []byte
	DirtyMask []bool
	ReqID     string
	ReqSrc    sim.RemotePort
	CycleLeft int
}

// vcPendingBottom tracks a request the VC sent downstream (to DRAM) while
// awaiting its response, so the response can be matched back to whichever
// top-side request triggered it.
type vcPendingBottom struct {
	BottomReqID string
	IsWrite     bool
	IsEviction  bool // Added to differentiate background evictions from normal write-throughs
	OrigAddress uint64
	ByteSize    uint64
	OrigReqID   string
	OrigReqSrc  sim.RemotePort
}

// VictimCache is a fully-associative victim cache sitting between an L1
// cache and main memory. It implements an "exclusive" policy: a hit
// invalidates the line in the VC (ownership moves back to L1), and any
// dirty line evicted from L1 arrives at the VC as a normal WriteReq, which
// the VC allocates a slot for (LRU replacement if full).
type VictimCache struct {
	*sim.TickingComponent

	TopPort    sim.Port
	BottomPort sim.Port
	LowModule  sim.RemotePort // DRAM (or whatever sits below the VC)

	spec   VCSpec
	Blocks []VCBlock

	topTransactions []vcTopTransaction
	pendingBottom   []vcPendingBottom

	cycleCount uint64

	VCHits          uint64
	VCMisses        uint64
	BottomSendCount uint64
}

// NewVictimCache creates a victim cache. Wire LowModule and plug TopPort /
// BottomPort into a connection exactly like any other akita component.
func NewVictimCache(engine sim.Engine, spec VCSpec) *VictimCache {
	vc := &VictimCache{spec: spec}

	vc.Blocks = make([]VCBlock, spec.NumBlocks)
	for i := range vc.Blocks {
		vc.Blocks[i].Data = make([]byte, spec.BlockSize)
	}

	vc.TickingComponent = sim.NewTickingComponent(
		"VictimCache", engine, spec.Freq, vc,
	)

	vc.TopPort = sim.NewPort(vc, 16, 16, "Top")
	vc.AddPort("Top", vc.TopPort)

	vc.BottomPort = sim.NewPort(vc, 16, 16, "Bottom")
	vc.AddPort("Bottom", vc.BottomPort)

	return vc
}

// Tick implements sim.Ticker. Order mirrors SeqAgent's Tick: drain responses
// from below first, then age in-flight top transactions, then accept a new
// top-side request, then try to complete whichever transaction is at the
// head of the queue.
func (vc *VictimCache) Tick() bool {
	progress := false

	progress = vc.processBottomResponse() || progress
	progress = vc.countDown() || progress
	progress = vc.receiveTopRequest() || progress
	progress = vc.processHeadTransaction() || progress

	return progress
}

func (vc *VictimCache) countDown() bool {
	progress := false
	for i := range vc.topTransactions {
		if vc.topTransactions[i].CycleLeft > 0 {
			vc.topTransactions[i].CycleLeft--
			progress = true
		}
	}
	return progress
}

func (vc *VictimCache) receiveTopRequest() bool {
	msgI := vc.TopPort.PeekIncoming()
	if msgI == nil {
		return false
	}

	switch req := msgI.(type) {
	case *mem.ReadReq:
		vc.topTransactions = append(vc.topTransactions, vcTopTransaction{
			IsRead:    true,
			Address:   req.Address,
			ByteSize:  req.AccessByteSize,
			ReqID:     req.ID,
			ReqSrc:    req.Src,
			CycleLeft: vc.spec.Latency,
		})
	case *mem.WriteReq:
		// Every WriteReq the VC receives here is a full dirty-line
		// eviction/writeback from the writeback L1 above it (the
		// stock akita writeback cache never forwards partial CPU
		// writes to the bottom port - only whole-block fetches and
		// whole-block eviction writebacks). So this always allocates
		// a slot, it never merely "updates if present".
		vc.topTransactions = append(vc.topTransactions, vcTopTransaction{
			IsWrite:   true,
			Address:   req.Address,
			Data:      req.Data,
			DirtyMask: req.DirtyMask,
			ReqID:     req.ID,
			ReqSrc:    req.Src,
			CycleLeft: vc.spec.Latency,
		})
	default:
		panic(fmt.Sprintf("VictimCache: unsupported top request type %T", msgI))
	}

	vc.TopPort.RetrieveIncoming()
	return true
}

func (vc *VictimCache) processHeadTransaction() bool {
	if len(vc.topTransactions) == 0 {
		return false
	}

	trans := vc.topTransactions[0]
	if trans.CycleLeft > 0 {
		return false
	}

	var handled bool
	if trans.IsRead {
		handled = vc.handleRead(trans)
	} else {
		handled = vc.handleWrite(trans)
	}

	if handled {
		vc.topTransactions = vc.topTransactions[1:]
	}

	return handled
}

func (vc *VictimCache) blockMask() uint64 {
	return uint64(vc.spec.BlockSize) - 1
}

func (vc *VictimCache) handleRead(trans vcTopTransaction) bool {
	alignedAddr := trans.Address &^ vc.blockMask()

	for i := range vc.Blocks {
		if vc.Blocks[i].Valid && vc.Blocks[i].Tag == alignedAddr {
			if !vc.TopPort.CanSend() {
				return false
			}

			vc.VCHits++

			// Exclusive property: once the line is handed back up to
			// L1, the VC no longer holds a copy.
			vc.Blocks[i].Valid = false

			offset := trans.Address & vc.blockMask()
			data := make([]byte, trans.ByteSize)
			copy(data, vc.Blocks[i].Data[offset:offset+trans.ByteSize])

			rsp := mem.DataReadyRspBuilder{}.
				WithSrc(vc.TopPort.AsRemote()).
				WithDst(trans.ReqSrc).
				WithRspTo(trans.ReqID).
				WithData(data).
				Build()

			vc.TopPort.Send(rsp)
			return true
		}
	}

	// Miss: forward to DRAM (or whatever LowModule is).
	if !vc.BottomPort.CanSend() {
		return false
	}

	vc.VCMisses++
	vc.BottomSendCount++

	bottomReq := mem.ReadReqBuilder{}.
		WithSrc(vc.BottomPort.AsRemote()).
		WithDst(vc.LowModule).
		WithAddress(alignedAddr).
		WithByteSize(uint64(vc.spec.BlockSize)).
		WithPID(vm.PID(1)).
		Build()

	vc.BottomPort.Send(bottomReq)
	vc.pendingBottom = append(vc.pendingBottom, vcPendingBottom{
		BottomReqID: bottomReq.ID,
		IsWrite:     false,
		IsEviction:  false,
		OrigAddress: trans.Address,
		ByteSize:    trans.ByteSize,
		OrigReqID:   trans.ReqID,
		OrigReqSrc:  trans.ReqSrc,
	})

	return true
}

func (vc *VictimCache) handleWrite(trans vcTopTransaction) bool {
	if !vc.TopPort.CanSend() {
		return false
	}

	alignedAddr := trans.Address &^ vc.blockMask()
	now := vc.cycleCount
	vc.cycleCount++

	// 1. Already resident -> refresh in place.
	for i := range vc.Blocks {
		if vc.Blocks[i].Valid && vc.Blocks[i].Tag == alignedAddr {
			copy(vc.Blocks[i].Data, trans.Data)
			vc.Blocks[i].LastAccessTime = now
			vc.ackWrite(trans)
			return true
		}
	}

	// 2. Empty slot available (e.g. one freed by a prior VC hit).
	for i := range vc.Blocks {
		if !vc.Blocks[i].Valid {
			vc.Blocks[i].Valid = true
			vc.Blocks[i].Tag = alignedAddr
			copy(vc.Blocks[i].Data, trans.Data)
			vc.Blocks[i].LastAccessTime = now
			vc.ackWrite(trans)
			return true
		}
	}

	// 3. Full -> LRU replacement.
	// Since we are replacing an old block, we must write it back to memory (DRAM).
	if !vc.BottomPort.CanSend() {
		return false // Stall until BottomPort can accept the eviction request
	}

	lruIdx := 0
	minTime := vc.Blocks[0].LastAccessTime
	for i := 1; i < len(vc.Blocks); i++ {
		if vc.Blocks[i].LastAccessTime < minTime {
			minTime = vc.Blocks[i].LastAccessTime
			lruIdx = i
		}
	}

	// Create and send the eviction write request to the memory level below
	evictedTag := vc.Blocks[lruIdx].Tag
	evictedData := make([]byte, vc.spec.BlockSize)
	copy(evictedData, vc.Blocks[lruIdx].Data)

	bottomReq := mem.WriteReqBuilder{}.
		WithSrc(vc.BottomPort.AsRemote()).
		WithDst(vc.LowModule).
		WithAddress(evictedTag).
		WithData(evictedData).
		WithPID(vm.PID(1)).
		Build()

	vc.BottomPort.Send(bottomReq)
	vc.BottomSendCount++

	// Track the pending bottom write so we can process its response
	vc.pendingBottom = append(vc.pendingBottom, vcPendingBottom{
		BottomReqID: bottomReq.ID,
		IsWrite:     true,
		IsEviction:  true, // Mark as an eviction so we don't reply to TopPort later
	})

	// Now that eviction is in flight, overwrite the slot with the new data
	vc.Blocks[lruIdx].Valid = true
	vc.Blocks[lruIdx].Tag = alignedAddr
	copy(vc.Blocks[lruIdx].Data, trans.Data)
	vc.Blocks[lruIdx].LastAccessTime = now
	
	// Acknowledge the top write immediately so L1 can proceed
	vc.ackWrite(trans)

	return true
}

// ackWrite completes the eviction write-back immediately: the akita
// writeback cache's writeBufferStage blocks waiting for a WriteDoneRsp
// before it considers the eviction slot free, so the VC must reply.
func (vc *VictimCache) ackWrite(trans vcTopTransaction) {
	rsp := mem.WriteDoneRspBuilder{}.
		WithSrc(vc.TopPort.AsRemote()).
		WithDst(trans.ReqSrc).
		WithRspTo(trans.ReqID).
		Build()

	vc.TopPort.Send(rsp)
}

func (vc *VictimCache) processBottomResponse() bool {
	msgI := vc.BottomPort.PeekIncoming()
	if msgI == nil {
		return false
	}

	switch rsp := msgI.(type) {
	case *mem.DataReadyRsp:
		return vc.completeBottomRead(rsp)
	case *mem.WriteDoneRsp:
		// Handles WriteDoneRsp from DRAM after a block eviction (or write-through)
		return vc.completeBottomWrite(rsp)
	default:
		panic(fmt.Sprintf("VictimCache: unsupported bottom message type %T", msgI))
	}
}

func (vc *VictimCache) completeBottomRead(rsp *mem.DataReadyRsp) bool {
	idx := -1
	for i, p := range vc.pendingBottom {
		if p.BottomReqID == rsp.RespondTo {
			idx = i
			break
		}
	}
	if idx == -1 {
		panic(fmt.Sprintf("VictimCache: DataReadyRsp for unknown request %s", rsp.RespondTo))
	}

	pending := vc.pendingBottom[idx]

	if !vc.TopPort.CanSend() {
		return false
	}

	data := make([]byte, pending.ByteSize)
	offset := pending.OrigAddress & vc.blockMask()
	copy(data, rsp.Data[offset:offset+pending.ByteSize])

	topRsp := mem.DataReadyRspBuilder{}.
		WithSrc(vc.TopPort.AsRemote()).
		WithDst(pending.OrigReqSrc).
		WithRspTo(pending.OrigReqID).
		WithData(data).
		Build()

	vc.TopPort.Send(topRsp)
	vc.pendingBottom = append(vc.pendingBottom[:idx], vc.pendingBottom[idx+1:]...)
	vc.BottomPort.RetrieveIncoming()

	return true
}

func (vc *VictimCache) completeBottomWrite(rsp *mem.WriteDoneRsp) bool {
	idx := -1
	for i, p := range vc.pendingBottom {
		if p.BottomReqID == rsp.RespondTo {
			idx = i
			break
		}
	}
	if idx == -1 {
		panic(fmt.Sprintf("VictimCache: WriteDoneRsp for unknown request %s", rsp.RespondTo))
	}

	pending := vc.pendingBottom[idx]

	// If it is a background eviction, there is no TopPort request to respond to.
	// Only forward the response upward if it was initiated by a top port request.
	if !pending.IsEviction {
		if !vc.TopPort.CanSend() {
			return false
		}

		topRsp := mem.WriteDoneRspBuilder{}.
			WithSrc(vc.TopPort.AsRemote()).
			WithDst(pending.OrigReqSrc).
			WithRspTo(pending.OrigReqID).
			Build()

		vc.TopPort.Send(topRsp)
	}

	vc.pendingBottom = append(vc.pendingBottom[:idx], vc.pendingBottom[idx+1:]...)
	vc.BottomPort.RetrieveIncoming()

	return true
}

// PrintVCStats prints victim-cache statistics, mirroring the style of the
// L1 stats already printed in SeqAgent.Tick.
// func PrintVCStats(vc *VictimCache) {
// 	fmt.Println("=====================================")
// 	fmt.Println("       VICTIM CACHE STATISTICS       ")
// 	fmt.Println("=====================================")
// 	fmt.Printf("VC Hits:              %d\n", vc.VCHits)
// 	fmt.Printf("VC Misses:            %d\n", vc.VCMisses)
// 	total := vc.VCHits + vc.VCMisses
// 	if total > 0 {
// 		hitRate := float64(vc.VCHits) / float64(total) * 100
// 		fmt.Printf("VC Hit Rate:          %.2f%%\n", hitRate)
// 	}
// 	fmt.Printf("Traffic to Memory:    %d reqs\n", vc.BottomSendCount)
// 	fmt.Println("=====================================")
// }