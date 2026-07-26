package main

import (
	"fmt"
	"log"

	mem "github.com/sarchlab/akita/v5/mem/memprotocol"
	"github.com/sarchlab/akita/v5/messaging"
	"github.com/sarchlab/akita/v5/modeling"
	"github.com/sarchlab/akita/v5/noc/directconnection"
	"github.com/sarchlab/akita/v5/timing"
)

// ============================================================================
// ۱. تعریف کامپوننت پردازنده (CPU / TestBench)
// ============================================================================

type CPUSpec struct {
	Freq timing.Freq `json:"freq"`
}

var defaultCPUSpec = CPUSpec{
	Freq: 1 * timing.GHz,
}

type MemoryAccess struct {
	IsWrite   bool   `json:"is_write"`
	Address   uint64 `json:"address"`
	WriteData []byte `json:"write_data"`
}

type CPUState struct {
	Accesses       []MemoryAccess       `json:"accesses"`
	CurrentIndex   int                  `json:"current_index"`
	PendingReqID   uint64               `json:"pending_req_id"`
	CompletedCount int                  `json:"completed_count"`
	DstPort        messaging.RemotePort `json:"-"`
}

type CPU = modeling.Component[CPUSpec, CPUState, modeling.None]

type CPUBuilder struct {
	spec      CPUSpec
	registrar modeling.Registrar
}

func MakeCPUBuilder() CPUBuilder {
	return CPUBuilder{spec: defaultCPUSpec}
}

func (b CPUBuilder) WithRegistrar(reg modeling.Registrar) CPUBuilder {
	b.registrar = reg
	return b
}

func (b CPUBuilder) Build(name string) *CPU {
	comp := modeling.NewBuilder[CPUSpec, CPUState, modeling.None]().
		WithEngine(b.registrar.GetEngine()).
		WithFreq(b.spec.Freq).
		WithSpec(b.spec).
		Build(name)

	comp.State = CPUState{
		Accesses: make([]MemoryAccess, 0),
	}

	comp.AddMiddleware(&cpuSendMW{comp: comp})
	comp.AddMiddleware(&cpuReceiveMW{comp: comp})

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

func cpuTopPort(comp *CPU) messaging.Port {
	return comp.GetPortByName("TopPort")
}

type cpuSendMW struct {
	comp *CPU
}

func (m *cpuSendMW) Tick() bool {
	state := &m.comp.State
	if state.PendingReqID != 0 || state.CurrentIndex >= len(state.Accesses) {
		return false
	}
	if !cpuTopPort(m.comp).CanSend() {
		return false
	}

	acc := state.Accesses[state.CurrentIndex]
	var req messaging.Msg

	if acc.IsWrite {
		writeReq := &mem.WriteReq{
			MsgMeta: messaging.MsgMeta{
				ID:  timing.GetIDGenerator().Generate(),
				Src: cpuTopPort(m.comp).AsRemote(),
				Dst: state.DstPort,
			},
			Address: acc.Address,
			Data:    acc.WriteData,
		}
		req = writeReq
		state.PendingReqID = writeReq.ID
	} else {
		readReq := &mem.ReadReq{
			MsgMeta: messaging.MsgMeta{
				ID:  timing.GetIDGenerator().Generate(),
				Src: cpuTopPort(m.comp).AsRemote(),
				Dst: state.DstPort,
			},
			Address:        acc.Address,
			AccessByteSize: 4,
		}
		req = readReq
		state.PendingReqID = readReq.ID
	}

	cpuTopPort(m.comp).Send(req)
	state.CurrentIndex++
	return true
}

type cpuReceiveMW struct {
	comp *CPU
}

func (m *cpuReceiveMW) Tick() bool {
	msgI := cpuTopPort(m.comp).PeekIncoming()
	if msgI == nil {
		return false
	}
	state := &m.comp.State

	switch rsp := msgI.(type) {
	case *mem.DataReadyRsp:
		if rsp.RspTo == state.PendingReqID {
			state.PendingReqID = 0
			state.CompletedCount++
			cpuTopPort(m.comp).RetrieveIncoming()
			return true
		}
	case *mem.WriteDoneRsp:
		if rsp.RspTo == state.PendingReqID {
			state.PendingReqID = 0
			state.CompletedCount++
			cpuTopPort(m.comp).RetrieveIncoming()
			return true
		}
	}
	return false
}

// ============================================================================
// ۲. تابع اصلی (Main) - اجرای فاز دوم با Victim Cache و مقایسه ترافیک
// ============================================================================

func main() {
	fmt.Println(">>> Starting Akita v5 Simulation (Phase 2: With Victim Cache) <<<")

	engine := timing.NewSerialEngine()
	registrar := modeling.NewStandaloneRegistrar(engine)

	// ۱. ساخت ۴ قطعه اصلی معماری
	memory := MakeMemoryBuilder().WithRegistrar(registrar).Build("MainMem")
	victimCache := MakeVCBuilder().WithRegistrar(registrar).Build("VictimCache")
	cache := MakeCacheBuilder().WithRegistrar(registrar).Build("L1Cache")
	cpu := MakeCPUBuilder().WithRegistrar(registrar).Build("CPU")

	// ۲. اتصال پورت‌ها: CPU <-> L1 <-> VC <-> Memory
	connTop := directconnection.MakeBuilder().WithRegistrar(registrar).Build("ConnTop")
	connTop.PlugIn(cpu.GetPortByName("TopPort"))
	connTop.PlugIn(cache.GetPortByName("TopPort"))

	connMiddle := directconnection.MakeBuilder().WithRegistrar(registrar).Build("ConnMiddle")
	connMiddle.PlugIn(cache.GetPortByName("BottomPort"))
	connMiddle.PlugIn(victimCache.GetPortByName("TopPort"))

	connBottom := directconnection.MakeBuilder().WithRegistrar(registrar).Build("ConnBottom")
	connBottom.PlugIn(victimCache.GetPortByName("BottomPort"))
	connBottom.PlugIn(memory.GetPortByName("TopPort"))

	// ۳. تنظیم آدرس ماژول‌های پایینی در هر کش
	cState := cache.State
	cState.LowModule = victimCache.GetPortByName("TopPort").AsRemote()
	cache.State = cState

	vcState := victimCache.State
	vcState.LowModule = memory.GetPortByName("TopPort").AsRemote()
	victimCache.State = vcState

	// ۴. بارگذاری دقیقاً همان بنچمارک فاز اول (برای مقایسه علمی دقیق)
	accesses := make([]MemoryAccess, 0)

	accesses = append(accesses, MemoryAccess{IsWrite: true, Address: 0x0000, WriteData: []byte{0x11, 0x11, 0x11, 0x11}})
	accesses = append(accesses, MemoryAccess{IsWrite: true, Address: 0x0100, WriteData: []byte{0x22, 0x22, 0x22, 0x22}})
	accesses = append(accesses, MemoryAccess{IsWrite: true, Address: 0x0050, WriteData: []byte{0xAA, 0xBB, 0xCC, 0xDD}})

	// حلقه Stride با تداخل ۱۰۰٪ در L1 (آدرس‌های 0x0000 و 0x0100)
	for i := 0; i < 6; i++ {
		accesses = append(accesses, MemoryAccess{IsWrite: false, Address: 0x0000})
		accesses = append(accesses, MemoryAccess{IsWrite: false, Address: 0x0100})
	}

	accesses = append(accesses, MemoryAccess{IsWrite: false, Address: 0x0050})
	accesses = append(accesses, MemoryAccess{IsWrite: false, Address: 0x0050})
	accesses = append(accesses, MemoryAccess{IsWrite: false, Address: 0x0050})
	accesses = append(accesses, MemoryAccess{IsWrite: false, Address: 0x0050})

	cpuState := cpu.State
	cpuState.DstPort = cache.GetPortByName("TopPort").AsRemote()
	cpuState.Accesses = accesses
	cpu.State = cpuState

	// ۵. اجرای شبیه‌سازی
	cpu.TickLater()
	err := engine.Run()
	if err != nil {
		log.Panicf("خطا در اجرای شبیه‌ساز: %v", err)
	}

	fmt.Println("\n>>> Simulation Completed Successfully! <<<")
	fmt.Printf("Total Time Elapsed: %.12f seconds\n\n", engine.CurrentTime())

	// ۶. چاپ آمار هر دو کش و مقایسه عملکرد
	PrintCacheStats(cache)
	PrintVCStats(victimCache)

	// ۷. محاسبه فرمول ارتقایافته AMAT طبق صورت پروژه:
	// AMAT = HitTime_L1 + MissRate_L1 * (HitRate_VC * Penalty_VC + MissRate_VC * Penalty_L2)
	l1 := &cache.State
	vc := &victimCache.State

	totalL1Reads := l1.ReadHits + l1.ReadMisses
	totalVCReads := vc.VCHits + vc.VCMisses

	if totalL1Reads > 0 && totalVCReads > 0 {
		missRateL1 := float64(l1.ReadMisses) / float64(totalL1Reads)
		hitRateVC := float64(vc.VCHits) / float64(totalVCReads)
		missRateVC := float64(vc.VCMisses) / float64(totalVCReads)

		hitTimeL1 := float64(cache.Spec().Latency)          // ۱ سیکل
		penaltyVC := float64(victimCache.Spec().Latency)    // ۲ سیکل
		penaltyL2 := float64(memory.Spec().Latency)         // ۱۰۰ سیکل

		amat := hitTimeL1 + missRateL1*(hitRateVC*penaltyVC+missRateVC*penaltyL2)

		fmt.Println("==================================================")
		fmt.Println("       ENHANCED AMAT MATHEMATICAL ANALYSIS        ")
		fmt.Println("==================================================")
		fmt.Printf("L1 Read Miss Rate:    %.2f%%\n", missRateL1*100)
		fmt.Printf("VC Hit Rate:          %.2f%%\n", hitRateVC*100)
		fmt.Printf("VC Miss Rate:         %.2f%%\n", missRateVC*100)
		fmt.Printf("L1 Hit Latency:       %.0f cycles\n", hitTimeL1)
		fmt.Printf("VC Access Penalty:    %.0f cycles\n", penaltyVC)
		fmt.Printf("Memory Miss Penalty:  %.0f cycles\n", penaltyL2)
		fmt.Println("--------------------------------------------------")
		fmt.Printf("Calculated AMAT:      %.2f cycles (Was 82.25 in Phase 1!)\n", amat)
		fmt.Println("==================================================")
	}

	fmt.Println()
	PrintMemoryDump(memory, 0x0000, 0x0010)
	PrintMemoryDump(memory, 0x0050, 0x0060)
	PrintMemoryDump(memory, 0x0100, 0x0110)
}
