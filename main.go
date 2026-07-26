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

type CPUSpec struct {
	Freq timing.Freq `json:"freq"`
}

var defaultCPUSpec = CPUSpec{Freq: 1 * timing.GHz}

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

	comp.State = CPUState{Accesses: make([]MemoryAccess, 0)}
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

func cpuTopPort(comp *CPU) messaging.Port { return comp.GetPortByName("TopPort") }

type cpuSendMW struct{ comp *CPU }

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

type cpuReceiveMW struct{ comp *CPU }

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

// stride 256
func generateBenchmark() []MemoryAccess {
	accesses := make([]MemoryAccess, 0)
	accesses = append(accesses, MemoryAccess{IsWrite: true, Address: 0x0000, WriteData: []byte{0x11, 0x11, 0x11, 0x11}})
	accesses = append(accesses, MemoryAccess{IsWrite: true, Address: 0x0100, WriteData: []byte{0x22, 0x22, 0x22, 0x22}})
	accesses = append(accesses, MemoryAccess{IsWrite: true, Address: 0x0050, WriteData: []byte{0xAA, 0xBB, 0xCC, 0xDD}})

	for i := 0; i < 6; i++ {
		accesses = append(accesses, MemoryAccess{IsWrite: false, Address: 0x0000})
		accesses = append(accesses, MemoryAccess{IsWrite: false, Address: 0x0100})
	}

	accesses = append(accesses, MemoryAccess{IsWrite: false, Address: 0x0050})
	accesses = append(accesses, MemoryAccess{IsWrite: false, Address: 0x0050})
	accesses = append(accesses, MemoryAccess{IsWrite: false, Address: 0x0050})
	accesses = append(accesses, MemoryAccess{IsWrite: false, Address: 0x0050})
	return accesses
}

type SimMetrics struct {
	ExecTime    float64
	L1Hits      uint64
	L1Misses    uint64
	VCHits      uint64
	VCMisses    uint64
	MemReads    uint64
	MemWrites   uint64
	MemTraffic  uint64
	AMAT        float64
}

func main() {
	fmt.Println("####################################################################")
	fmt.Println("#         AKITA v5 CACHE ARCHITECTURE SIMULATION & ANALYSIS        #")
	fmt.Println("####################################################################\n")

	// L1 cache
	fmt.Println(">>> [1/2] Running Phase 1: Base Architecture (Direct-Mapped L1 -> Memory)...")
	engine1 := timing.NewSerialEngine()
	reg1 := modeling.NewStandaloneRegistrar(engine1)

	mem1 := MakeMemoryBuilder().WithRegistrar(reg1).Build("MainMem1")
	l1_1 := MakeCacheBuilder().WithRegistrar(reg1).Build("L1Cache1")
	cpu1 := MakeCPUBuilder().WithRegistrar(reg1).Build("CPU1")

	connTop1 := directconnection.MakeBuilder().WithRegistrar(reg1).Build("ConnTop1")
	connTop1.PlugIn(cpu1.GetPortByName("TopPort"))
	connTop1.PlugIn(l1_1.GetPortByName("TopPort"))

	connBot1 := directconnection.MakeBuilder().WithRegistrar(reg1).Build("ConnBot1")
	connBot1.PlugIn(l1_1.GetPortByName("BottomPort"))
	connBot1.PlugIn(mem1.GetPortByName("TopPort"))

	s1 := l1_1.State
	s1.LowModule = mem1.GetPortByName("TopPort").AsRemote()
	s1.HasVC = false
	l1_1.State = s1

	cs1 := cpu1.State
	cs1.DstPort = l1_1.GetPortByName("TopPort").AsRemote()
	cs1.Accesses = generateBenchmark()
	cpu1.State = cs1

	cpu1.TickLater()
	if err := engine1.Run(); err != nil {
		log.Panic(err)
	}

	st1 := &l1_1.State
	m1_reads := st1.BottomReadReqs
	m1_writes := st1.BottomWriteReqs
	m1_traffic := m1_reads + m1_writes
	
	totalL1_1 := st1.ReadHits + st1.ReadMisses
	missRate1 := float64(st1.ReadMisses) / float64(totalL1_1)
	amat1 := float64(l1_1.Spec().Latency) + (missRate1 * float64(mem1.Spec().Latency))

	metrics1 := SimMetrics{
		ExecTime: float64(engine1.CurrentTime()), L1Hits: st1.ReadHits + st1.WriteHits, L1Misses: st1.ReadMisses + st1.WriteMisses,
		VCHits: 0, VCMisses: 0, MemReads: m1_reads, MemWrites: m1_writes, MemTraffic: m1_traffic, AMAT: amat1,
	}

	// Victim Cache
	fmt.Println(">>> [2/2] Running Phase 2: With Victim Cache (L1 -> VC -> Memory)...")
	engine2 := timing.NewSerialEngine()
	reg2 := modeling.NewStandaloneRegistrar(engine2)

	mem2 := MakeMemoryBuilder().WithRegistrar(reg2).Build("MainMem2")
	vc2 := MakeVCBuilder().WithRegistrar(reg2).Build("VictimCache2")
	l1_2 := MakeCacheBuilder().WithRegistrar(reg2).Build("L1Cache2")
	cpu2 := MakeCPUBuilder().WithRegistrar(reg2).Build("CPU2")

	connTop2 := directconnection.MakeBuilder().WithRegistrar(reg2).Build("ConnTop2")
	connTop2.PlugIn(cpu2.GetPortByName("TopPort"))
	connTop2.PlugIn(l1_2.GetPortByName("TopPort"))

	connMid2 := directconnection.MakeBuilder().WithRegistrar(reg2).Build("ConnMid2")
	connMid2.PlugIn(l1_2.GetPortByName("BottomPort"))
	connMid2.PlugIn(vc2.GetPortByName("TopPort"))

	connBot2 := directconnection.MakeBuilder().WithRegistrar(reg2).Build("ConnBot2")
	connBot2.PlugIn(vc2.GetPortByName("BottomPort"))
	connBot2.PlugIn(mem2.GetPortByName("TopPort"))

	s2 := l1_2.State
	s2.LowModule = vc2.GetPortByName("TopPort").AsRemote()
	s2.HasVC = true // swap
	l1_2.State = s2

	vs2 := vc2.State
	vs2.LowModule = mem2.GetPortByName("TopPort").AsRemote()
	vc2.State = vs2

	cs2 := cpu2.State
	cs2.DstPort = l1_2.GetPortByName("TopPort").AsRemote()
	cs2.Accesses = generateBenchmark()
	cpu2.State = cs2

	cpu2.TickLater()
	if err := engine2.Run(); err != nil {
		log.Panic(err)
	}

	st2_l1 := &l1_2.State
	st2_vc := &vc2.State
	
	m2_reads := st2_vc.VCMisses
	m2_writes := st2_vc.BottomSendCount - st2_vc.VCMisses
	m2_traffic := st2_vc.BottomSendCount

	totalL1_2 := st2_l1.ReadHits + st2_l1.ReadMisses
	totalVC_2 := st2_vc.VCHits + st2_vc.VCMisses

	missRateL1_2 := float64(st2_l1.ReadMisses) / float64(totalL1_2)
	hitRateVC_2 := float64(st2_vc.VCHits) / float64(totalVC_2)
	missRateVC_2 := float64(st2_vc.VCMisses) / float64(totalVC_2)

	hitTimeL1 := float64(l1_2.Spec().Latency)
	penaltyVC := float64(vc2.Spec().Latency)
	penaltyMem := float64(mem2.Spec().Latency)

	amat2 := hitTimeL1 + missRateL1_2*(hitRateVC_2*penaltyVC+missRateVC_2*penaltyMem)

	metrics2 := SimMetrics{
		ExecTime: float64(engine2.CurrentTime()), L1Hits: st2_l1.ReadHits + st2_l1.WriteHits, L1Misses: st2_l1.ReadMisses + st2_l1.WriteMisses,
		VCHits: st2_vc.VCHits, VCMisses: st2_vc.VCMisses, MemReads: m2_reads, MemWrites: m2_writes, MemTraffic: m2_traffic, AMAT: amat2,
	}

	fmt.Println("\n####################################################################")
	fmt.Println("#                 FINAL ARCHITECTURAL COMPARISON                   #")
	fmt.Println("####################################################################")
	fmt.Printf("%-26s | %-18s | %-18s\n", "Metric / Parameter", "Phase 1 (Base)", "Phase 2 (With VC)")
	fmt.Println("---------------------------+--------------------+--------------------")
	fmt.Printf("%-26s | %-18.12f | %-18.12f\n", "1. Execution Time (sec)", metrics1.ExecTime, metrics2.ExecTime)
	fmt.Printf("%-26s | %-18d | %-18d\n", "2. L1 Cache Hits", metrics1.L1Hits, metrics2.L1Hits)
	fmt.Printf("%-26s | %-18d | %-18d\n", "   L1 Cache Misses", metrics1.L1Misses, metrics2.L1Misses)
	fmt.Printf("%-26s | %-18s | %-18d\n", "3. Victim Cache Hits", "N/A", metrics2.VCHits)
	fmt.Printf("%-26s | %-18s | %-18d\n", "   Victim Cache Misses", "N/A", metrics2.VCMisses)
	fmt.Printf("%-26s | %-18d | %-18d\n", "4. Memory Read Requests", metrics1.MemReads, metrics2.MemReads)
	fmt.Printf("%-26s | %-18d | %-18d\n", "   Memory Write Requests", metrics1.MemWrites, metrics2.MemWrites)
	fmt.Printf("%-26s | %-18d | %-18d\n", "5. Total Memory Traffic", metrics1.MemTraffic, metrics2.MemTraffic)
	fmt.Println("---------------------------+--------------------+--------------------")
	fmt.Printf("%-26s | %-18.2f | %-18.2f\n", "6. Calculated AMAT (cycl)", metrics1.AMAT, metrics2.AMAT)
	fmt.Println("####################################################################")
	
	trafficReduction := (float64(metrics1.MemTraffic - metrics2.MemTraffic) / float64(metrics1.MemTraffic)) * 100
	amatReduction := (float64(metrics1.AMAT - metrics2.AMAT) / float64(metrics1.AMAT)) * 100
	fmt.Printf("\n>>> CONCLUSION & ANALYSIS FOR REPORT:\n")
	fmt.Printf("* Adding Victim Cache reduced Main Memory Traffic by %.2f%%!\n", trafficReduction)
	fmt.Printf("* Average Memory Access Time (AMAT) improved by %.2f%%!\n", amatReduction)
	fmt.Println("####################################################################")
}