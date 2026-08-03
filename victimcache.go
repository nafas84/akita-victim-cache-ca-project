package main

import (
	"fmt"

	"github.com/sarchlab/akita/v4/mem/mem"
	"github.com/sarchlab/akita/v4/mem/vm"
	"github.com/sarchlab/akita/v4/sim"
)

type VCSpec struct {
	Freq      sim.Freq
	Latency   int
	BlockSize int // Equal L1 block size
	NumBlocks int 
}

func DefaultVCSpec() VCSpec {
	return VCSpec{
		Freq:      1 * sim.GHz,
		Latency:   5,
		BlockSize: 64,
		NumBlocks: 8,
	}
}

type VCBlock struct {
	Tag            uint64
	Valid          bool
	Dirty          bool
	Data           []byte
	LastAccessTime uint64 // LRU Replacement
}

type vcTopTransaction struct {
	IsRead    bool
	IsWrite   bool // ???
	Address   uint64
	ByteSize  uint64 // ???
	Data      []byte
	ReqID     string // ???
	ReqSrc    sim.RemotePort 
	CycleLeft int
}

type vcPendingBottom struct {
	BottomReqID string

	IsWrite     bool
	IsEviction  bool // ???
	OrigAddress uint64
	ByteSize    uint64
	OrigReqID   string
	OrigReqSrc  sim.RemotePort
}

type VictimCache struct {
	*sim.TickingComponent

	TopPort    sim.Port
	BottomPort sim.Port
	LowModule  sim.RemotePort 

	spec   VCSpec
	Blocks []VCBlock

	topTransactions []vcTopTransaction // Queue Lookup
	pendingBottom   []vcPendingBottom // req to M.M

	cycleCount uint64 // ???

	// Statistic
	VCHits          uint64
	VCMisses        uint64
	BottomSendCount uint64
}

// constructor victim cache
func NewVictimCache(engine sim.Engine, spec VCSpec) *VictimCache {
	vc := &VictimCache{spec: spec}

	vc.Blocks = make([]VCBlock, spec.NumBlocks)
	for i := range vc.Blocks {
		vc.Blocks[i].Data = make([]byte, spec.BlockSize)
	}

	// new ticking component
	vc.TickingComponent = sim.NewTickingComponent(
		"VictimCache", engine, spec.Freq, vc,
	)

	// vc ports
	vc.TopPort = sim.NewPort(vc, 16, 16, "Top")
	vc.AddPort("Top", vc.TopPort)

	vc.BottomPort = sim.NewPort(vc, 16, 16, "Bottom")
	vc.AddPort("Bottom", vc.BottomPort)

	return vc
}

// TickingComponent (Ticker interface)
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
		if vc.topTransactions[i].CycleLeft > 0 { // latency
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
		vc.topTransactions = append(vc.topTransactions, vcTopTransaction{
			IsWrite:   true,
			Address:   req.Address,
			Data:      req.Data,
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
			vc.Blocks[i].LastAccessTime = vc.cycleCount
			vc.cycleCount++

			// Exclusive property: once the line is handed back up to
			// L1, the VC no longer holds a copy.
			vc.Blocks[i].Valid = false
			vc.Blocks[i].Dirty = false

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
			vc.Blocks[i].Dirty = true
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
			vc.Blocks[i].Dirty = true
			vc.Blocks[i].LastAccessTime = now
			vc.ackWrite(trans)
			return true
		}
	}

	// 3. Full -> LRU replacement.
	// Since we are replacing an old block, we must write it back to memory (DRAM).

	lruIdx := 0
	minTime := vc.Blocks[0].LastAccessTime
	for i := 1; i < len(vc.Blocks); i++ {
		if vc.Blocks[i].LastAccessTime < minTime {
			minTime = vc.Blocks[i].LastAccessTime
			lruIdx = i
		}
	}

	if vc.Blocks[lruIdx].Dirty {
		if !vc.BottomPort.CanSend() {
			return false // Stall until BottomPort can accept the eviction request
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
	}

	// Now that eviction is in flight, overwrite the slot with the new data
	vc.Blocks[lruIdx].Valid = true
	vc.Blocks[lruIdx].Tag = alignedAddr
	vc.Blocks[lruIdx].Dirty = true
	copy(vc.Blocks[lruIdx].Data, trans.Data)
	vc.Blocks[lruIdx].LastAccessTime = now
	
	// Acknowledge the top write immediately so L1 can proceed
	vc.ackWrite(trans)

	return true
}

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
