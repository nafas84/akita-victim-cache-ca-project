package main

import (
	"fmt"

	"github.com/sarchlab/akita/v4/mem/cache/writeback"
	"github.com/sarchlab/akita/v4/mem/idealmemcontroller"
	"github.com/sarchlab/akita/v4/mem/mem"
	"github.com/sarchlab/akita/v4/sim"
	"github.com/sarchlab/akita/v4/sim/directconnection"
)

func main() {
	runBenchmark("Small Benchmark", false)
    runBenchmark("Large Benchmark", true)

    runBenchmarkWithVC("Small Benchmark (with Victim Cache)", false)
    runBenchmarkWithVC("Large Benchmark (with Victim Cache)", true)
}


func runBenchmark(name string, large bool) {
    fmt.Println()
    fmt.Println("===================================")
    fmt.Println(name)
    fmt.Println("===================================")

    engine := sim.NewSerialEngine()

    conn := directconnection.MakeBuilder().
        WithEngine(engine).
        WithFreq(1 * sim.GHz).
        Build("Conn")

    dram := idealmemcontroller.MakeBuilder().
        WithEngine(engine).
        WithNewStorage(64 * mem.KB).
        Build("DRAM")

    mapper := &mem.SinglePortMapper{}

    cache := writeback.MakeBuilder().
        WithEngine(engine).
        WithAddressToPortMapper(mapper).
        WithByteSize(1 * mem.KB).
        WithLog2BlockSize(6).
        WithWayAssociativity(1).
        WithNumMSHREntry(4).
        Build("L1")

    mapper.Port = dram.GetPortByName("Top").AsRemote()

    agent := NewTraceCPU(engine, large)
    agent.Cache = cache
    agent.LowModule = cache.GetPortByName("Top")

    conn.PlugIn(agent.GetPortByName("Mem"))
    conn.PlugIn(cache.GetPortByName("Top"))
    conn.PlugIn(cache.GetPortByName("Bottom"))
    conn.PlugIn(dram.GetPortByName("Top"))

    agent.TickLater()

    if err := engine.Run(); err != nil {
        panic(err)
    }
}

// runBenchmarkWithVC is identical to runBenchmark, except a VictimCache is
// inserted between the L1 and DRAM: CPU (SeqAgent) -> L1 -> VictimCache -> DRAM.
func runBenchmarkWithVC(name string, large bool) {
    fmt.Println()
    fmt.Println("===================================")
    fmt.Println(name)
    fmt.Println("===================================")

    engine := sim.NewSerialEngine()

    conn := directconnection.MakeBuilder().
        WithEngine(engine).
        WithFreq(1 * sim.GHz).
        Build("Conn")

    dram := idealmemcontroller.MakeBuilder().
        WithEngine(engine).
        WithNewStorage(64 * mem.KB).
        Build("DRAM")

    mapper := &mem.SinglePortMapper{}

    cache := writeback.MakeBuilder().
        WithEngine(engine).
        WithAddressToPortMapper(mapper).
        WithByteSize(1 * mem.KB).
        WithLog2BlockSize(6).
        WithWayAssociativity(1).
        WithNumMSHREntry(4).
        Build("L1")

    // Victim cache sits below the L1. Its block size must match the L1's
    // block size (64 bytes, i.e. Log2BlockSize=6), which is what
    // DefaultVCSpec() already provides.
    vc := NewVictimCache(engine, DefaultVCSpec())
    vc.LowModule = dram.GetPortByName("Top").AsRemote()

    // L1 misses / evictions now go to the victim cache instead of straight
    // to DRAM.
    mapper.Port = vc.TopPort.AsRemote()

    agent := NewTraceCPU(engine, large)
    agent.Cache = cache
    agent.VictimCache = vc
    agent.LowModule = cache.GetPortByName("Top")

    conn.PlugIn(agent.GetPortByName("Mem"))
    conn.PlugIn(cache.GetPortByName("Top"))
    conn.PlugIn(cache.GetPortByName("Bottom"))
    conn.PlugIn(vc.TopPort)
    conn.PlugIn(vc.BottomPort)
    conn.PlugIn(dram.GetPortByName("Top"))

    agent.TickLater()

    if err := engine.Run(); err != nil {
        panic(err)
    }

    //PrintVCStats(vc)
}