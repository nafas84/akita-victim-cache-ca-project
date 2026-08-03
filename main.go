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
	runBenchmark("Test 1", false)
    runBenchmarkWithVC("Test 1 (with Victim Cache)", false)

    runBenchmark("Test 2", true)
    runBenchmarkWithVC("Test 2 (with Victim Cache)", true)
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

    memory := idealmemcontroller.MakeBuilder().
        WithEngine(engine).
        WithNewStorage(64 * mem.KB).
        Build("Memory")

    mapper := &mem.SinglePortMapper{}

    cache := writeback.MakeBuilder().
        WithEngine(engine).
        WithAddressToPortMapper(mapper).
        WithByteSize(1 * mem.KB).
        WithLog2BlockSize(6).
        WithWayAssociativity(1).
        WithNumMSHREntry(4).
        Build("L1")

    mapper.Port = memory.GetPortByName("Top").AsRemote()

    agent := NewTraceCPU(engine, large)
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
    fmt.Println()
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
        Build("Memory")

    mapper := &mem.SinglePortMapper{}

    cache := writeback.MakeBuilder().
        WithEngine(engine).
        WithAddressToPortMapper(mapper).
        WithByteSize(1 * mem.KB).
        WithLog2BlockSize(6).
        WithWayAssociativity(1).
        WithNumMSHREntry(4).
        Build("L1")

    vc := NewVictimCache(engine, DefaultVCSpec())
    vc.LowModule = memory.GetPortByName("Top").AsRemote()

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
    conn.PlugIn(memory.GetPortByName("Top"))

    agent.TickLater()

    if err := engine.Run(); err != nil {
        panic(err)
    }

    //PrintVCStats(vc)
}