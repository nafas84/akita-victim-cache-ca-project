package main

import (
	"log"

	mem "github.com/sarchlab/akita/v5/mem/memprotocol"
	"github.com/sarchlab/akita/v5/messaging"
	"github.com/sarchlab/akita/v5/modeling"
	"github.com/sarchlab/akita/v5/timing"
)

type CacheSpec struct {
	Freq      timing.Freq `json:"freq"`
	Latency   int         `json:"latency"`
	BlockSize int         `json:"block_size"`
	NumBlocks int         `json:"num_blocks"`
}

var defaultCacheSpec = CacheSpec{
	Freq:      1 * timing.GHz,
	Latency:   1,
	BlockSize: 16,
	NumBlocks: 16,
}

func DefaultCacheSpec() CacheSpec {
	return defaultCacheSpec
}

type CacheBlock struct {
	Tag   uint64 `json:"tag"`
	Valid bool   `json:"valid"`
	Data  []byte `json:"data"`
}

type topTransaction struct {
	IsWrite        bool                 `json:"is_write"`
	Address        uint64               `json:"address"`
	AccessByteSize uint64               `json:"access_byte_size"`
	Data           []byte               `json:"data"`
	DirtyMask      []bool               `json:"dirty_mask"`
	ReqID          uint64               `json:"req_id"`
	ReqSrc         messaging.RemotePort `json:"req_src"`
	CycleLeft      int                  `json:"cycle_left"`
}

type pendingMissOrWrite struct {
	BottomReqID    uint64               `json:"bottom_req_id"`
	IsWrite        bool                 `json:"is_write"`
	OrigAddress    uint64               `json:"orig_address"`
	AccessByteSize uint64               `json:"access_byte_size"`
	OrigReqID      uint64               `json:"orig_req_id"`
	OrigReqSrc     messaging.RemotePort `json:"orig_req_src"`
}

type CacheState struct {
	Blocks          []CacheBlock         `json:"-"`
	TopTransactions []topTransaction     `json:"-"`
	PendingBottom   []pendingMissOrWrite `json:"-"`
	LowModule       messaging.RemotePort `json:"-"`
	HasVC           bool                 `json:"has_vc"` // phase 1,2

	ReadHits        uint64 `json:"read_hits"`
	ReadMisses      uint64 `json:"read_misses"`
	WriteHits       uint64 `json:"write_hits"`
	WriteMisses     uint64 `json:"write_misses"`
	
	BottomReadReqs  uint64 `json:"bottom_read_reqs"`
	BottomWriteReqs uint64 `json:"bottom_write_reqs"`
}

type Cache = modeling.Component[CacheSpec, CacheState, modeling.None]

type CacheBuilder struct {
	spec      CacheSpec
	registrar modeling.Registrar
}

func MakeCacheBuilder() CacheBuilder {
	return CacheBuilder{spec: defaultCacheSpec}
}

func (b CacheBuilder) WithRegistrar(reg modeling.Registrar) CacheBuilder {
	b.registrar = reg
	return b
}

func (b CacheBuilder) WithSpec(spec CacheSpec) CacheBuilder {
	b.spec = spec
	return b
}

func (b CacheBuilder) Build(name string) *Cache {
	if b.registrar == nil {
		panic("Cache: WithRegistrar is required")
	}

	comp := modeling.NewBuilder[CacheSpec, CacheState, modeling.None]().
		WithEngine(b.registrar.GetEngine()).
		WithFreq(b.spec.Freq).
		WithSpec(b.spec).
		Build(name)

	blocks := make([]CacheBlock, b.spec.NumBlocks)
	for i := range blocks {
		blocks[i].Data = make([]byte, b.spec.BlockSize)
	}

	comp.State = CacheState{
		Blocks:          blocks,
		TopTransactions: make([]topTransaction, 0),
		PendingBottom:   make([]pendingMissOrWrite, 0),
	}

	comp.AddMiddleware(&cacheBottomProcessMW{comp: comp})
	comp.AddMiddleware(&cacheTopProcessMW{comp: comp})
	comp.AddMiddleware(&cacheTopReceiveMW{comp: comp})

	comp.DeclarePort("TopPort")
	topPort := modeling.MakePortBuilder().
		WithRegistrar(b.registrar).
		WithComponent(comp).
		WithSpec(modeling.PortSpec{BufSize: 16}).
		Build("TopPort")
	comp.AssignPort("TopPort", topPort)

	comp.DeclarePort("BottomPort")
	bottomPort := modeling.MakePortBuilder().
		WithRegistrar(b.registrar).
		WithComponent(comp).
		WithSpec(modeling.PortSpec{BufSize: 16}).
		Build("BottomPort")
	comp.AssignPort("BottomPort", bottomPort)

	b.registrar.RegisterComponent(comp)
	return comp
}

func cacheTopPort(comp *Cache) messaging.Port {
	return comp.GetPortByName("TopPort")
}

func cacheBottomPort(comp *Cache) messaging.Port {
	return comp.GetPortByName("BottomPort")
}

func decodeAddress(addr uint64) (tag, index, offset uint64) {
	offset = addr & 0xF
	index = (addr >> 4) & 0xF
	tag = addr >> 8
	return
}

type cacheTopReceiveMW struct {
	comp *Cache
}

func (m *cacheTopReceiveMW) Tick() bool {
	madeProgress := false
	madeProgress = m.countDown() || madeProgress
	madeProgress = m.processInput() || madeProgress
	return madeProgress
}

func (m *cacheTopReceiveMW) countDown() bool {
	state := &m.comp.State
	madeProgress := false
	for i := range state.TopTransactions {
		if state.TopTransactions[i].CycleLeft > 0 {
			state.TopTransactions[i].CycleLeft--
			madeProgress = true
		}
	}
	return madeProgress
}

func (m *cacheTopReceiveMW) processInput() bool {
	msgI := cacheTopPort(m.comp).PeekIncoming()
	if msgI == nil {
		return false
	}

	state := &m.comp.State
	spec := m.comp.Spec()

	switch msg := msgI.(type) {
	case *mem.ReadReq:
		trans := topTransaction{
			IsWrite:        false,
			Address:        msg.Address,
			AccessByteSize: msg.AccessByteSize,
			ReqID:          msg.ID,
			ReqSrc:         msg.Src,
			CycleLeft:      spec.Latency,
		}
		state.TopTransactions = append(state.TopTransactions, trans)
		cacheTopPort(m.comp).RetrieveIncoming()
		return true
	case *mem.WriteReq:
		trans := topTransaction{
			IsWrite:   true,
			Address:   msg.Address,
			Data:      msg.Data,
			DirtyMask: msg.DirtyMask,
			ReqID:     msg.ID,
			ReqSrc:    msg.Src,
			CycleLeft: spec.Latency,
		}
		state.TopTransactions = append(state.TopTransactions, trans)
		cacheTopPort(m.comp).RetrieveIncoming()
		return true
	default:
		log.Panicf("error port top %T", msgI)
	}
	return false
}

type cacheTopProcessMW struct {
	comp *Cache
}

func (m *cacheTopProcessMW) Tick() bool {
	state := &m.comp.State
	if len(state.TopTransactions) == 0 {
		return false
	}
	trans := state.TopTransactions[0]
	if trans.CycleLeft > 0 {
		return false
	}

	if trans.IsWrite {
		if m.handleWrite(trans) {
			state.TopTransactions = state.TopTransactions[1:]
			return true
		}
	} else {
		if m.handleRead(trans) {
			state.TopTransactions = state.TopTransactions[1:]
			return true
		}
	}
	return false
}

func (m *cacheTopProcessMW) handleRead(trans topTransaction) bool {
	state := &m.comp.State
	tag, index, offset := decodeAddress(trans.Address)
	block := &state.Blocks[index]

	if block.Valid && block.Tag == tag {
		if !cacheTopPort(m.comp).CanSend() {
			return false
		}
		state.ReadHits++

		data := make([]byte, trans.AccessByteSize)
		copy(data, block.Data[offset:offset+trans.AccessByteSize])

		rsp := &mem.DataReadyRsp{
			MsgMeta: messaging.MsgMeta{
				ID:    timing.GetIDGenerator().Generate(),
				Src:   cacheTopPort(m.comp).AsRemote(),
				Dst:   trans.ReqSrc,
				RspTo: trans.ReqID,
			},
			Data: data,
		}
		cacheTopPort(m.comp).Send(rsp)
		return true
	}

	if !cacheBottomPort(m.comp).CanSend() {
		return false
	}
	state.ReadMisses++
	state.BottomReadReqs++

	alignedAddr := trans.Address &^ 0xF

	bottomReq := &mem.ReadReq{
		MsgMeta: messaging.MsgMeta{
			ID:  timing.GetIDGenerator().Generate(),
			Src: cacheBottomPort(m.comp).AsRemote(),
			Dst: state.LowModule,
		},
		Address:        alignedAddr,
		AccessByteSize: 16,
	}

	cacheBottomPort(m.comp).Send(bottomReq)
	state.PendingBottom = append(state.PendingBottom, pendingMissOrWrite{
		BottomReqID:    bottomReq.ID,
		IsWrite:        false,
		OrigAddress:    trans.Address,
		AccessByteSize: trans.AccessByteSize,
		OrigReqID:      trans.ReqID,
		OrigReqSrc:     trans.ReqSrc,
	})
	return true
}

func (m *cacheTopProcessMW) handleWrite(trans topTransaction) bool {
	state := &m.comp.State
	tag, index, offset := decodeAddress(trans.Address)
	block := &state.Blocks[index]

	if !cacheBottomPort(m.comp).CanSend() {
		return false
	}

	if block.Valid && block.Tag == tag {
		state.WriteHits++
		for i := 0; i < len(trans.Data); i++ {
			if trans.DirtyMask == nil || trans.DirtyMask[i] {
				block.Data[int(offset)+i] = trans.Data[i]
			}
		}
	} else {
		state.WriteMisses++
	}

	state.BottomWriteReqs++

	bottomReq := &mem.WriteReq{
		MsgMeta: messaging.MsgMeta{
			ID:  timing.GetIDGenerator().Generate(),
			Src: cacheBottomPort(m.comp).AsRemote(),
			Dst: state.LowModule,
		},
		Address:   trans.Address,
		Data:      trans.Data,
		DirtyMask: trans.DirtyMask,
	}

	cacheBottomPort(m.comp).Send(bottomReq)
	state.PendingBottom = append(state.PendingBottom, pendingMissOrWrite{
		BottomReqID: bottomReq.ID,
		IsWrite:     true,
		OrigReqID:   trans.ReqID,
		OrigReqSrc:  trans.ReqSrc,
	})
	return true
}

type cacheBottomProcessMW struct {
	comp *Cache
}

func (m *cacheBottomProcessMW) Tick() bool {
	msgI := cacheBottomPort(m.comp).PeekIncoming()
	if msgI == nil {
		return false
	}

	state := &m.comp.State

	switch rsp := msgI.(type) {
	case *mem.DataReadyRsp:
		return m.handleDataReady(rsp, state)
	case *mem.WriteDoneRsp:
		return m.handleWriteDone(rsp, state)
	default:
		log.Panicf("پیام ناشناخته در BottomPort کش: %T", msgI)
	}
	return false
}

// swap
func (m *cacheBottomProcessMW) handleDataReady(rsp *mem.DataReadyRsp, state *CacheState) bool {
	idx := -1
	for i, p := range state.PendingBottom {
		if p.BottomReqID == rsp.RspTo {
			idx = i
			break
		}
	}
	if idx == -1 {
		log.Panicf("error! %d", rsp.RspTo)
	}

	pending := state.PendingBottom[idx]
	tag, index, offset := decodeAddress(pending.OrigAddress)
	block := &state.Blocks[index]

	// Downward expulsion is only done if we are in phase 2 (with victim cache)!
	if block.Valid && state.HasVC {
		if !cacheTopPort(m.comp).CanSend() || !cacheBottomPort(m.comp).CanSend() {
			return false // (Deadlock)
		}

		evictReq := &EvictToVCReq{
			MsgMeta: messaging.MsgMeta{
				ID:  timing.GetIDGenerator().Generate(),
				Src: cacheBottomPort(m.comp).AsRemote(),
				Dst: state.LowModule,
			},
			Address: (block.Tag << 8) | (uint64(index) << 4),
			Data:    make([]byte, 16),
		}
		copy(evictReq.Data, block.Data)
		cacheBottomPort(m.comp).Send(evictReq)
	} else {
		if !cacheTopPort(m.comp).CanSend() {
			return false
		}
	}

	block.Valid = true
	block.Tag = tag
	copy(block.Data, rsp.Data)

	data := make([]byte, pending.AccessByteSize)
	copy(data, block.Data[offset:offset+pending.AccessByteSize])

	topRsp := &mem.DataReadyRsp{
		MsgMeta: messaging.MsgMeta{
			ID:    timing.GetIDGenerator().Generate(),
			Src:   cacheTopPort(m.comp).AsRemote(),
			Dst:   pending.OrigReqSrc,
			RspTo: pending.OrigReqID,
		},
		Data: data,
	}

	cacheTopPort(m.comp).Send(topRsp)
	state.PendingBottom = append(state.PendingBottom[:idx], state.PendingBottom[idx+1:]...)
	cacheBottomPort(m.comp).RetrieveIncoming()
	return true
}

func (m *cacheBottomProcessMW) handleWriteDone(rsp *mem.WriteDoneRsp, state *CacheState) bool {
	idx := -1
	for i, p := range state.PendingBottom {
		if p.BottomReqID == rsp.RspTo {
			idx = i
			break
		}
	}
	if idx == -1 {
		log.Panicf("error! %d", rsp.RspTo)
	}

	pending := state.PendingBottom[idx]

	if !cacheTopPort(m.comp).CanSend() {
		return false
	}

	topRsp := &mem.WriteDoneRsp{
		MsgMeta: messaging.MsgMeta{
			ID:    timing.GetIDGenerator().Generate(),
			Src:   cacheTopPort(m.comp).AsRemote(),
			Dst:   pending.OrigReqSrc,
			RspTo: pending.OrigReqID,
		},
	}

	cacheTopPort(m.comp).Send(topRsp)
	state.PendingBottom = append(state.PendingBottom[:idx], state.PendingBottom[idx+1:]...)
	cacheBottomPort(m.comp).RetrieveIncoming()
	return true
}