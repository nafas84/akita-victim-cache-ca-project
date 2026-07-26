
package main

import (
	"fmt"
	"log"

	mem "github.com/sarchlab/akita/v5/mem/memprotocol"
	"github.com/sarchlab/akita/v5/messaging"
	"github.com/sarchlab/akita/v5/modeling"
	"github.com/sarchlab/akita/v5/timing"
)

// ============================================================================
// ۱. تعریف پیام اخراج از L1 به Victim Cache (Eviction Message)
// ============================================================================

type EvictToVCReq struct {
	messaging.MsgMeta
	Address uint64
	Data    []byte
}

// ============================================================================
// ۲. تنظیمات ثابت (Spec) و ساختار بافر ۸ بلاکی و وضعیت (State)
// ============================================================================

type VCSpec struct {
	Freq      timing.Freq `json:"freq"`
	Latency   int         `json:"latency"`    // تاخیر دسترسی بسیار سریع (مثلا ۲ سیکل)
	BlockSize int         `json:"block_size"` // ۱۶ بایت
	NumBlocks int         `json:"num_blocks"` // ۸ بلاک (تمام انجمنی)
}

var defaultVCSpec = VCSpec{
	Freq:      1 * timing.GHz,
	Latency:   2,
	BlockSize: 16,
	NumBlocks: 8, // تضمین ظرفیت ۸ بلاک
}

func DefaultVCSpec() VCSpec {
	return defaultVCSpec
}

// VCBlock ساختار یک خط در ویکتیم کش (Fully Associative -> Tag آدرس کامل بلاک است)
type VCBlock struct {
	Tag            uint64 `json:"tag"`
	Valid          bool   `json:"valid"`
	Data           []byte `json:"data"`
	LastAccessTime uint64 `json:"last_access_time"` // برای پیاده‌سازی LRU
}

type vcTopTransaction struct {
	IsRead         bool                 `json:"is_read"`
	IsWrite        bool                 `json:"is_write"`
	IsEvict        bool                 `json:"is_evict"`
	Address        uint64               `json:"address"`
	AccessByteSize uint64               `json:"access_byte_size"`
	Data           []byte               `json:"data"`
	DirtyMask      []bool               `json:"dirty_mask"`
	ReqID          uint64               `json:"req_id"`
	ReqSrc         messaging.RemotePort `json:"req_src"`
	CycleLeft      int                  `json:"cycle_left"`
}

type vcPendingBottom struct {
	BottomReqID    uint64               `json:"bottom_req_id"`
	IsWrite        bool                 `json:"is_write"`
	OrigAddress    uint64               `json:"orig_address"`
	AccessByteSize uint64               `json:"access_byte_size"`
	OrigReqID      uint64               `json:"orig_req_id"`
	OrigReqSrc     messaging.RemotePort `json:"orig_req_src"`
}

type VCState struct {
	Blocks          []VCBlock          `json:"-"`
	TopTransactions []vcTopTransaction `json:"-"`
	PendingBottom   []vcPendingBottom  `json:"-"`
	LowModule       messaging.RemotePort `json:"-"`

	VCHits          uint64 `json:"vc_hits"`
	VCMisses        uint64 `json:"vc_misses"`
	EvictionRcvd    uint64 `json:"eviction_rcvd"`
	BottomSendCount uint64 `json:"bottom_send_count"`
}

type VictimCache = modeling.Component[VCSpec, VCState, modeling.None]

// ============================================================================
// ۳. الگوی Builder برای ساخت کامپوننت Victim Cache
// ============================================================================

type VCBuilder struct {
	spec      VCSpec
	registrar modeling.Registrar
}

func MakeVCBuilder() VCBuilder {
	return VCBuilder{spec: defaultVCSpec}
}

func (b VCBuilder) WithRegistrar(reg modeling.Registrar) VCBuilder {
	b.registrar = reg
	return b
}

func (b VCBuilder) WithSpec(spec VCSpec) VCBuilder {
	b.spec = spec
	return b
}

func (b VCBuilder) Build(name string) *VictimCache {
	if b.registrar == nil {
		panic("VictimCache: WithRegistrar is required")
	}

	comp := modeling.NewBuilder[VCSpec, VCState, modeling.None]().
		WithEngine(b.registrar.GetEngine()).
		WithFreq(b.spec.Freq).
		WithSpec(b.spec).
		Build(name)

	blocks := make([]VCBlock, b.spec.NumBlocks)
	for i := range blocks {
		blocks[i].Data = make([]byte, b.spec.BlockSize)
	}

	comp.State = VCState{
		Blocks:          blocks,
		TopTransactions: make([]vcTopTransaction, 0),
		PendingBottom:   make([]vcPendingBottom, 0),
	}

	comp.AddMiddleware(&vcBottomProcessMW{comp: comp})
	comp.AddMiddleware(&vcTopProcessMW{comp: comp})
	comp.AddMiddleware(&vcTopReceiveMW{comp: comp})

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

func vcTopPort(comp *VictimCache) messaging.Port {
	return comp.GetPortByName("TopPort")
}

func vcBottomPort(comp *VictimCache) messaging.Port {
	return comp.GetPortByName("BottomPort")
}

// ============================================================================
// ۴. Middleware دریافت پیام از TopPort (از سمت L1 Cache)
// ============================================================================

type vcTopReceiveMW struct {
	comp *VictimCache
}

func (m *vcTopReceiveMW) Tick() bool {
	madeProgress := false
	madeProgress = m.countDown() || madeProgress
	madeProgress = m.processInput() || madeProgress
	return madeProgress
}

func (m *vcTopReceiveMW) countDown() bool {
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

func (m *vcTopReceiveMW) processInput() bool {
	msgI := vcTopPort(m.comp).PeekIncoming()
	if msgI == nil {
		return false
	}

	state := &m.comp.State
	spec := m.comp.Spec()

	switch msg := msgI.(type) {
	case *mem.ReadReq:
		trans := vcTopTransaction{
			IsRead:         true,
			Address:        msg.Address,
			AccessByteSize: msg.AccessByteSize,
			ReqID:          msg.ID,
			ReqSrc:         msg.Src,
			CycleLeft:      spec.Latency,
		}
		state.TopTransactions = append(state.TopTransactions, trans)
		vcTopPort(m.comp).RetrieveIncoming()
		return true
	case *mem.WriteReq:
		trans := vcTopTransaction{
			IsWrite:   true,
			Address:   msg.Address,
			Data:      msg.Data,
			DirtyMask: msg.DirtyMask,
			ReqID:     msg.ID,
			ReqSrc:    msg.Src,
			CycleLeft: spec.Latency,
		}
		state.TopTransactions = append(state.TopTransactions, trans)
		vcTopPort(m.comp).RetrieveIncoming()
		return true
	case *EvictToVCReq:
		trans := vcTopTransaction{
			IsEvict:   true,
			Address:   msg.Address,
			Data:      msg.Data,
			CycleLeft: 1,
		}
		state.TopTransactions = append(state.TopTransactions, trans)
		vcTopPort(m.comp).RetrieveIncoming()
		return true
	default:
		log.Panicf("پیام ناشناخته در TopPort ویکتیم کش: %T", msgI)
	}
	return false
}

// ============================================================================
// ۵. Middleware پردازش منطق ویکتیم کش (LRU Replacement & Hit/Miss Logic)
// ============================================================================

type vcTopProcessMW struct {
	comp *VictimCache
}

func (m *vcTopProcessMW) Tick() bool {
	state := &m.comp.State
	if len(state.TopTransactions) == 0 {
		return false
	}
	trans := state.TopTransactions[0]
	if trans.CycleLeft > 0 {
		return false
	}

	if trans.IsEvict {
		m.handleEviction(trans)
		state.TopTransactions = state.TopTransactions[1:]
		return true
	} else if trans.IsRead {
		if m.handleRead(trans) {
			state.TopTransactions = state.TopTransactions[1:]
			return true
		}
	} else if trans.IsWrite {
		if m.handleWrite(trans) {
			state.TopTransactions = state.TopTransactions[1:]
			return true
		}
	}
	return false
}

func (m *vcTopProcessMW) handleEviction(trans vcTopTransaction) {
	state := &m.comp.State
	state.EvictionRcvd++

	alignedAddr := trans.Address &^ 0xF
	now := uint64(m.comp.CurrentTime())

	for i := range state.Blocks {
		if state.Blocks[i].Valid && state.Blocks[i].Tag == alignedAddr {
			copy(state.Blocks[i].Data, trans.Data)
			state.Blocks[i].LastAccessTime = now
			return
		}
	}

	for i := range state.Blocks {
		if !state.Blocks[i].Valid {
			state.Blocks[i].Valid = true
			state.Blocks[i].Tag = alignedAddr
			copy(state.Blocks[i].Data, trans.Data)
			state.Blocks[i].LastAccessTime = now
			return
		}
	}

	lruIdx := 0
	minTime := state.Blocks[0].LastAccessTime
	for i := 1; i < len(state.Blocks); i++ {
		if state.Blocks[i].LastAccessTime < minTime {
			minTime = state.Blocks[i].LastAccessTime
			lruIdx = i
		}
	}

	state.Blocks[lruIdx].Valid = true
	state.Blocks[lruIdx].Tag = alignedAddr
	copy(state.Blocks[lruIdx].Data, trans.Data)
	state.Blocks[lruIdx].LastAccessTime = now
}

func (m *vcTopProcessMW) handleRead(trans vcTopTransaction) bool {
	state := &m.comp.State
	alignedAddr := trans.Address &^ 0xF
	now := uint64(m.comp.CurrentTime())

	// نگاشت تمام‌انجمنی (Fully Associative): بررسی کل بافر بدون شاخص مجموعه
	for i := range state.Blocks {
		if state.Blocks[i].Valid && state.Blocks[i].Tag == alignedAddr {
			if !vcTopPort(m.comp).CanSend() {
				return false
			}
			state.VCHits++
			state.Blocks[i].LastAccessTime = now

			data := make([]byte, trans.AccessByteSize)
			offset := trans.Address & 0xF
			copy(data, state.Blocks[i].Data[offset:offset+trans.AccessByteSize])

			rsp := &mem.DataReadyRsp{
				MsgMeta: messaging.MsgMeta{
					ID:    timing.GetIDGenerator().Generate(),
					Src:   vcTopPort(m.comp).AsRemote(),
					Dst:   trans.ReqSrc,
					RspTo: trans.ReqID,
				},
				Data: data,
			}
			vcTopPort(m.comp).Send(rsp)
			return true
		}
	}

	if !vcBottomPort(m.comp).CanSend() {
		return false
	}
	state.VCMisses++
	state.BottomSendCount++

	bottomReq := &mem.ReadReq{
		MsgMeta: messaging.MsgMeta{
			ID:  timing.GetIDGenerator().Generate(),
			Src: vcBottomPort(m.comp).AsRemote(),
			Dst: state.LowModule,
		},
		Address:        alignedAddr,
		AccessByteSize: 16,
	}

	vcBottomPort(m.comp).Send(bottomReq)
	state.PendingBottom = append(state.PendingBottom, vcPendingBottom{
		BottomReqID:    bottomReq.ID,
		IsWrite:        false,
		OrigAddress:    trans.Address,
		AccessByteSize: trans.AccessByteSize,
		OrigReqID:      trans.ReqID,
		OrigReqSrc:     trans.ReqSrc,
	})
	return true
}

func (m *vcTopProcessMW) handleWrite(trans vcTopTransaction) bool {
	state := &m.comp.State
	alignedAddr := trans.Address &^ 0xF
	now := uint64(m.comp.CurrentTime())

	if !vcBottomPort(m.comp).CanSend() {
		return false
	}

	for i := range state.Blocks {
		if state.Blocks[i].Valid && state.Blocks[i].Tag == alignedAddr {
			offset := trans.Address & 0xF
			for j := 0; j < len(trans.Data); j++ {
				if trans.DirtyMask == nil || trans.DirtyMask[j] {
					state.Blocks[i].Data[int(offset)+j] = trans.Data[j]
				}
			}
			state.Blocks[i].LastAccessTime = now
			break
		}
	}

	state.BottomSendCount++

	// سیاست Write-Through: ارسال مستقیم و بدون تاخیر نوشتن به سطح حافظه پایینی
	bottomReq := &mem.WriteReq{
		MsgMeta: messaging.MsgMeta{
			ID:  timing.GetIDGenerator().Generate(),
			Src: vcBottomPort(m.comp).AsRemote(),
			Dst: state.LowModule,
		},
		Address:   trans.Address,
		Data:      trans.Data,
		DirtyMask: trans.DirtyMask,
	}

	vcBottomPort(m.comp).Send(bottomReq)
	state.PendingBottom = append(state.PendingBottom, vcPendingBottom{
		BottomReqID: bottomReq.ID,
		IsWrite:     true,
		OrigReqID:   trans.ReqID,
		OrigReqSrc:  trans.ReqSrc,
	})
	return true
}

// ============================================================================
// ۶. Middleware دریافت پاسخ از BottomPort و هدایت به سمت L1
// ============================================================================

type vcBottomProcessMW struct {
	comp *VictimCache
}

func (m *vcBottomProcessMW) Tick() bool {
	msgI := vcBottomPort(m.comp).PeekIncoming()
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
		log.Panicf("پیام ناشناخته در BottomPort ویکتیم کش: %T", msgI)
	}
	return false
}

func (m *vcBottomProcessMW) handleDataReady(rsp *mem.DataReadyRsp, state *VCState) bool {
	idx := -1
	for i, p := range state.PendingBottom {
		if p.BottomReqID == rsp.RspTo {
			idx = i
			break
		}
	}
	if idx == -1 {
		log.Panicf("پاسخ خواندن در VC بدون درخواست اولیه: %d", rsp.RspTo)
	}
	pending := state.PendingBottom[idx]

	if !vcTopPort(m.comp).CanSend() {
		return false
	}

	data := make([]byte, pending.AccessByteSize)
	offset := pending.OrigAddress & 0xF
	copy(data, rsp.Data[offset:offset+pending.AccessByteSize])

	topRsp := &mem.DataReadyRsp{
		MsgMeta: messaging.MsgMeta{
			ID:    timing.GetIDGenerator().Generate(),
			Src:   vcTopPort(m.comp).AsRemote(),
			Dst:   pending.OrigReqSrc,
			RspTo: pending.OrigReqID,
		},
		Data: data,
	}

	vcTopPort(m.comp).Send(topRsp)
	state.PendingBottom = append(state.PendingBottom[:idx], state.PendingBottom[idx+1:]...)
	vcBottomPort(m.comp).RetrieveIncoming()
	return true
}

func (m *vcBottomProcessMW) handleWriteDone(rsp *mem.WriteDoneRsp, state *VCState) bool {
	idx := -1
	for i, p := range state.PendingBottom {
		if p.BottomReqID == rsp.RspTo {
			idx = i
			break
		}
	}
	if idx == -1 {
		log.Panicf("پاسخ نوشتن در VC بدون درخواست اولیه: %d", rsp.RspTo)
	}
	pending := state.PendingBottom[idx]

	if !vcTopPort(m.comp).CanSend() {
		return false
	}

	topRsp := &mem.WriteDoneRsp{
		MsgMeta: messaging.MsgMeta{
			ID:    timing.GetIDGenerator().Generate(),
			Src:   vcTopPort(m.comp).AsRemote(),
			Dst:   pending.OrigReqSrc,
			RspTo: pending.OrigReqID,
		},
	}

	vcTopPort(m.comp).Send(topRsp)
	state.PendingBottom = append(state.PendingBottom[:idx], state.PendingBottom[idx+1:]...)
	vcBottomPort(m.comp).RetrieveIncoming()
	return true
}

func PrintVCStats(vc *VictimCache) {
	state := &vc.State
	fmt.Println("=====================================")
	fmt.Println("       VICTIM CACHE STATISTICS       ")
	fmt.Println("=====================================")
	fmt.Printf("VC Hits:              %d\n", state.VCHits)
	fmt.Printf("VC Misses:            %d\n", state.VCMisses)
	fmt.Printf("Evictions Received:   %d\n", state.EvictionRcvd)
	totalVC := state.VCHits + state.VCMisses
	if totalVC > 0 {
		hitRate := float64(state.VCHits) / float64(totalVC) * 100
		fmt.Printf("VC Hit Rate:          %.2f%%\n", hitRate)
	}
	fmt.Printf("Traffic to Memory:    %d reqs\n", state.BottomSendCount)
	fmt.Println("=====================================")
}