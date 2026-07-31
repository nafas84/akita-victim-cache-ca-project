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

    agent := NewSeqAgent(engine, large)
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