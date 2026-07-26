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
// ۱. تنظیمات ثابت (Spec) و وضعیت در حال اجرا (State)
// ============================================================================

type Spec struct {
	Freq     timing.Freq `json:"freq"`
	Capacity uint64      `json:"capacity"` // ظرفیت: 2^16 = 65536 بایت
	Latency  int         `json:"latency"`  // تاخیر دسترسی به تعداد سیکل کلاک
}

var defaultMemSpec = Spec{
	Freq:     1 * timing.GHz,
	Capacity: 65536, // 64 KB
	Latency:  100,   // 100 سیکل تاخیر پیش‌فرض
}

func DefaultMemSpec() Spec {
	return defaultMemSpec
}

type memTransaction struct {
	IsWrite        bool                 `json:"is_write"`
	Address        uint64               `json:"address"`
	AccessByteSize uint64               `json:"access_byte_size"`
	Data           []byte               `json:"data"`
	DirtyMask      []bool               `json:"dirty_mask"`
	ReqID          uint64               `json:"req_id"`
	ReqSrc         messaging.RemotePort `json:"req_src"`
	CycleLeft      int                  `json:"cycle_left"`
}

type State struct {
	Data         []byte           `json:"-"` // آرایه ۶۴ کیلوبایتی حافظه
	Transactions []memTransaction `json:"transactions"`
}

type Memory = modeling.Component[Spec, State, modeling.None]

// ============================================================================
// ۲. الگوی Builder برای ساخت کامپوننت حافظه
// ============================================================================

type MemoryBuilder struct {
	spec      Spec
	registrar modeling.Registrar
}

func MakeMemoryBuilder() MemoryBuilder {
	return MemoryBuilder{spec: defaultMemSpec}
}

func (b MemoryBuilder) WithRegistrar(reg modeling.Registrar) MemoryBuilder {
	b.registrar = reg
	return b
}

func (b MemoryBuilder) WithSpec(spec Spec) MemoryBuilder {
	b.spec = spec
	return b
}

func (b MemoryBuilder) Build(name string) *Memory {
	if b.registrar == nil {
		panic("Memory: WithRegistrar is required")
	}

	comp := modeling.NewBuilder[Spec, State, modeling.None]().
		WithEngine(b.registrar.GetEngine()).
		WithFreq(b.spec.Freq).
		WithSpec(b.spec).
		Build(name)

	comp.State = State{
		Data:         make([]byte, b.spec.Capacity),
		Transactions: make([]memTransaction, 0),
	}

	comp.AddMiddleware(&memSendMW{comp: comp})
	comp.AddMiddleware(&memReceiveProcessMW{comp: comp})

	// تعریف و تخصیص همزمان بافر پورت بالایی
	comp.DeclarePort("TopPort")
	topPort := modeling.MakePortBuilder().
		WithRegistrar(b.registrar).
		WithComponent(comp).
		WithSpec(modeling.PortSpec{BufSize: 16}).
		Build("TopPort")
	comp.AssignPort("TopPort", topPort)

	b.registrar.RegisterComponent(comp)
	return comp
}

func memTopPort(comp *Memory) messaging.Port {
	return comp.GetPortByName("TopPort")
}

// ============================================================================
// ۳. Middleware دریافت پیام و کاهش تاخیر (Receive & Countdown)
// ============================================================================

type memReceiveProcessMW struct {
	comp *Memory
}

func (m *memReceiveProcessMW) Tick() bool {
	madeProgress := false
	madeProgress = m.countDown() || madeProgress
	madeProgress = m.processInput() || madeProgress
	return madeProgress
}

func (m *memReceiveProcessMW) countDown() bool {
	state := &m.comp.State
	madeProgress := false

	for i := range state.Transactions {
		if state.Transactions[i].CycleLeft > 0 {
			state.Transactions[i].CycleLeft--
			madeProgress = true
		}
	}
	return madeProgress
}

func (m *memReceiveProcessMW) processInput() bool {
	msgI := memTopPort(m.comp).PeekIncoming()
	if msgI == nil {
		return false
	}

	state := &m.comp.State
	spec := m.comp.Spec()

	switch msg := msgI.(type) {
	case *mem.ReadReq:
		trans := memTransaction{
			IsWrite:        false,
			Address:        msg.Address,
			AccessByteSize: msg.AccessByteSize,
			ReqID:          msg.ID,
			ReqSrc:         msg.Src,
			CycleLeft:      spec.Latency,
		}
		state.Transactions = append(state.Transactions, trans)
		memTopPort(m.comp).RetrieveIncoming()
		return true
	case *mem.WriteReq:
		trans := memTransaction{
			IsWrite:   true,
			Address:   msg.Address,
			Data:      msg.Data,
			DirtyMask: msg.DirtyMask,
			ReqID:     msg.ID,
			ReqSrc:    msg.Src,
			CycleLeft: spec.Latency,
		}
		state.Transactions = append(state.Transactions, trans)
		memTopPort(m.comp).RetrieveIncoming()
		return true
	default:
		log.Panicf("پیام ناشناخته در حافظه: %T", msgI)
	}
	return false
}

// ============================================================================
// ۴. Middleware انجام کار و ارسال پاسخ (Send Response)
// ============================================================================

type memSendMW struct {
	comp *Memory
}

func (m *memSendMW) Tick() bool {
	state := &m.comp.State

	if len(state.Transactions) == 0 {
		return false
	}
	trans := state.Transactions[0]
	if trans.CycleLeft > 0 {
		return false
	}

	if !memTopPort(m.comp).CanSend() {
		return false
	}

	if trans.IsWrite {
		m.handleWrite(trans)
	} else {
		m.handleRead(trans)
	}

	state.Transactions = state.Transactions[1:]
	return true
}

func (m *memSendMW) handleRead(trans memTransaction) {
	state := &m.comp.State
	spec := m.comp.Spec()

	if trans.Address+trans.AccessByteSize > spec.Capacity {
		log.Panicf("خطای Out of Bounds: آدرس %d خارج از ظرفیت حافظه است.", trans.Address)
	}

	data := make([]byte, trans.AccessByteSize)
	start := int(trans.Address)
	end := start + int(trans.AccessByteSize)
	copy(data, state.Data[start:end])

	rsp := &mem.DataReadyRsp{
		MsgMeta: messaging.MsgMeta{
			ID:    timing.GetIDGenerator().Generate(),
			Src:   memTopPort(m.comp).AsRemote(),
			Dst:   trans.ReqSrc,
			RspTo: trans.ReqID,
		},
		Data: data,
	}

	memTopPort(m.comp).Send(rsp)
}

func (m *memSendMW) handleWrite(trans memTransaction) {
	state := &m.comp.State
	spec := m.comp.Spec()

	if trans.Address+uint64(len(trans.Data)) > spec.Capacity {
		log.Panicf("خطای Out of Bounds: آدرس %d خارج از ظرفیت حافظه است.", trans.Address)
	}

	for i := 0; i < len(trans.Data); i++ {
		if trans.DirtyMask == nil || trans.DirtyMask[i] {
			state.Data[int(trans.Address)+i] = trans.Data[i]
		}
	}

	rsp := &mem.WriteDoneRsp{
		MsgMeta: messaging.MsgMeta{
			ID:    timing.GetIDGenerator().Generate(),
			Src:   memTopPort(m.comp).AsRemote(),
			Dst:   trans.ReqSrc,
			RspTo: trans.ReqID,
		},
	}

	memTopPort(m.comp).Send(rsp)
}

func PrintMemoryDump(c *Memory, startAddr, endAddr uint64) {
	spec := c.Spec()
	state := &c.State

	fmt.Printf("--- Memory Dump [0x%X - 0x%X] ---\n", startAddr, endAddr)
	for i := startAddr; i < endAddr; i += 4 {
		if i+4 <= spec.Capacity {
			val := state.Data[int(i) : int(i)+4]
			fmt.Printf("Addr 0x%04X: %02X %02X %02X %02X\n", i, val[0], val[1], val[2], val[3])
		}
	}
	fmt.Println("---------------------------------")
}