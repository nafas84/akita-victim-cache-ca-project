package main

import (
	"fmt"

	"github.com/sarchlab/akita/v4/mem/cache/writeback"
	"github.com/sarchlab/akita/v4/mem/idealmemcontroller"
	"github.com/sarchlab/akita/v4/mem/mem"
	"github.com/sarchlab/akita/v4/sim"
	"github.com/sarchlab/akita/v4/sim/directconnection"
)

const (
	L1_LATENCY  = 1
	VC_LATENCY  = 5
	MEM_LATENCY = 100
)

func main() {
	runBenchmark("Test 1", false)
	runBenchmarkWithVC("Test 1 (with Victim Cache)", false)

	//runBenchmark("Test 2", true)
	//runBenchmarkWithVC("Test 2 (with Victim Cache)", true)
}

func runBenchmark(name string, large bool) {
	//fmt.Println()
	fmt.Println("===================================")
	fmt.Println(name)
	fmt.Println("===================================")

	engine := sim.NewSerialEngine()

	conn := directconnection.MakeBuilder().
		WithEngine(engine).
		WithFreq(1 * sim.GHz).
		Build("Conn")

	memory := idealmemcontroller.MakeBuilder().
		WithEngine(engine).
		WithNewStorage(64 * mem.KB).
		WithLatency(MEM_LATENCY).
		Build("Memory")

	mapper := &mem.SinglePortMapper{}

	cache := writeback.MakeBuilder().
		WithEngine(engine).
		WithAddressToPortMapper(mapper).
		WithByteSize(1 * mem.KB).
		WithLog2BlockSize(6).
		WithWayAssociativity(1).
		WithNumMSHREntry(4).
        WithDirectoryLatency(L1_LATENCY/2).
        WithBankLatency(L1_LATENCY/2).
		Build("L1")

	mapper.Port = memory.GetPortByName("Top").AsRemote()

	agent := NewTraceCPU(engine, DefaultCPUSpec, large, float64(L1_LATENCY), float64(VC_LATENCY), float64(MEM_LATENCY))
	agent.Cache = cache
	agent.LowModule = cache.GetPortByName("Top")

	conn.PlugIn(agent.GetPortByName("Mem"))
	conn.PlugIn(cache.GetPortByName("Top"))
	conn.PlugIn(cache.GetPortByName("Bottom"))
	conn.PlugIn(memory.GetPortByName("Top"))

	agent.TickLater()

	if err := engine.Run(); err != nil {
		panic(err)
	}
}

func runBenchmarkWithVC(name string, large bool) {
	//fmt.Println()
	//fmt.Println("===================================")
	fmt.Println(name)
	fmt.Println("===================================")

	engine := sim.NewSerialEngine()

	conn := directconnection.MakeBuilder().
		WithEngine(engine).
		WithFreq(1 * sim.GHz).
		Build("Conn")

	memory := idealmemcontroller.MakeBuilder().
		WithEngine(engine).
		WithNewStorage(64 * mem.KB).
		WithLatency(MEM_LATENCY).
		Build("Memory")

	mapper := &mem.SinglePortMapper{}

	cache := writeback.MakeBuilder().
		WithEngine(engine).
		WithAddressToPortMapper(mapper).
		WithByteSize(1 * mem.KB).
		WithLog2BlockSize(6).
		WithWayAssociativity(1).
		WithNumMSHREntry(4).
        WithDirectoryLatency(L1_LATENCY/2).
        WithBankLatency(L1_LATENCY/2).
		Build("L1")

	vcSpec := DefaultVCSpec()
	vcSpec.Latency = VC_LATENCY
	vc := NewVictimCache(engine, vcSpec)
	
	vc.LowModule = memory.GetPortByName("Top").AsRemote()

	mapper.Port = vc.TopPort.AsRemote()

	agent := NewTraceCPU(engine, DefaultCPUSpec, large, float64(L1_LATENCY), float64(VC_LATENCY), float64(MEM_LATENCY))
	agent.Cache = cache
	agent.VictimCache = vc
	agent.LowModule = cache.GetPortByName("Top")

	conn.PlugIn(agent.GetPortByName("Mem"))
	conn.PlugIn(cache.GetPortByName("Top"))
	conn.PlugIn(cache.GetPortByName("Bottom"))
	conn.PlugIn(vc.TopPort)
	conn.PlugIn(vc.BottomPort)
	conn.PlugIn(memory.GetPortByName("Top"))

	agent.TickLater()

	if err := engine.Run(); err != nil {
		panic(err)
	}

	//PrintVCStats(vc)
}