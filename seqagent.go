package main

import (
	"fmt"
	"encoding/binary"

	"github.com/sarchlab/akita/v4/mem/cache/writeback"
	"github.com/sarchlab/akita/v4/mem/mem"
	"github.com/sarchlab/akita/v4/sim"
)

type SeqAgent struct {
	*sim.TickingComponent

	LowModule sim.Port

	memPort sim.Port

	step int

	totalWrites int
	totalReads  int

	writeTrace []uint64
	readTrace  []uint64

	writeIndex int
	readIndex  int

	completedWrites int
	completedReads  int
	finished        bool

	pendingWrites map[string]*mem.WriteReq
	pendingReads  map[string]*mem.ReadReq	

	Cache *writeback.Comp
}

func NewSeqAgent(engine sim.Engine) *SeqAgent {

	a := &SeqAgent{
		pendingWrites: make(map[string]*mem.WriteReq),
		pendingReads:  make(map[string]*mem.ReadReq),
	}

	trace := []uint64{
		// Warm up
		0x0000,
		0x0040,
		0x0080,
		0x00C0,

		// These all map to the same cache line
		0x0000,
		0x8000,
		0x0000,
		0x10000,
		0x0000,
		0x8000,
		0x0000,
		0x10000,

		// Another region
		0x0400,
		0x0440,
		0x0480,
		0x04C0,

		// Reuse them (should hit)
		0x0400,
		0x0440,
		0x0480,

		// More thrashing
		0x0000,
		0x8000,
		0x10000,
		0x0000,
	}

	// Repeat the pattern many times.
	for i := 0; i < 50; i++ {
		a.writeTrace = append(a.writeTrace, trace...)
	}

	for i := 0; i < 50; i++ {
		a.readTrace = append(a.readTrace, trace...)
	}

	a.totalWrites = len(a.writeTrace)
	a.totalReads = len(a.readTrace)

	a.TickingComponent =
		sim.NewTickingComponent(
			"SeqAgent",
			engine,
			1*sim.GHz,
			a,
		)

	a.memPort = sim.NewPort(a, 4, 4, "Mem")
	a.AddPort("Mem", a.memPort)

	return a
}

func (a *SeqAgent) Tick() bool {
	if a.finished {
		return false
	}

	progress := false

	// Always process responses first.
	progress = a.processResponses() || progress

	// Only issue a new request if there is no outstanding request.
	if !a.hasPendingRequest() {
		if a.totalWrites > 0 {
			progress = a.sendWrite() || progress
		} else if a.totalReads > 0 {
			progress = a.sendRead() || progress
		}
	}

	if a.totalWrites == 0 &&
		a.totalReads == 0 &&
		!a.hasPendingRequest() {

		fmt.Println()
		fmt.Println("====================================")
		fmt.Println("Benchmark Finished Successfully")
		fmt.Printf("Writes        : %d\n", a.completedWrites)
		fmt.Printf("Reads         : %d\n", a.completedReads)
		fmt.Printf("Read Hits     : %d\n", a.Cache.ReadHit)
		fmt.Printf("Read Misses   : %d\n", a.Cache.ReadMiss)
		fmt.Printf("Write Hits    : %d\n", a.Cache.WriteHit)
		fmt.Printf("Write Misses  : %d\n", a.Cache.WriteMiss)
		fmt.Printf("Evictions     : %d\n", a.Cache.Evictions)
		fmt.Printf("Write Backs   : %d\n", a.Cache.WriteBack)

		totalReads := a.Cache.ReadHit + a.Cache.ReadMiss
		if totalReads > 0 {
			fmt.Printf(
				"Read Hit Rate : %.2f%%\n",
				100*float64(a.Cache.ReadHit)/float64(totalReads),
			)
		}

		totalWrites := a.Cache.WriteHit + a.Cache.WriteMiss
		if totalWrites > 0 {
			fmt.Printf(
				"Write Hit Rate: %.2f%%\n",
				100*float64(a.Cache.WriteHit)/float64(totalWrites),
			)
		}

		fmt.Println("====================================")

		a.finished = true
	}

	return progress
}

func (a *SeqAgent) processResponses() bool {

    msg := a.memPort.RetrieveIncoming()
    if msg == nil {
        return false
    }

    switch rsp := msg.(type) {

    case *mem.WriteDoneRsp:
        delete(a.pendingWrites, rsp.RespondTo)

        a.completedWrites++


    case *mem.DataReadyRsp:
        delete(a.pendingReads, rsp.RespondTo)

        a.completedReads++

        
    }

    return true
}

func (a *SeqAgent) hasPendingRequest() bool {
	return len(a.pendingWrites) > 0 || len(a.pendingReads) > 0
}


func (a *SeqAgent) sendWrite() bool {

    if a.writeIndex >= len(a.writeTrace) {
        return false
    }

    addr := a.writeTrace[a.writeIndex]

    data := make([]byte, 8)
    binary.LittleEndian.PutUint64(data, addr)

    req := mem.WriteReqBuilder{}.
        WithSrc(a.memPort.AsRemote()).
        WithDst(a.LowModule.AsRemote()).
        WithAddress(addr).
        WithPID(1).
        WithData(data).
        Build()

    if err := a.memPort.Send(req); err != nil {
        return false
    }

    a.pendingWrites[req.ID] = req


    a.writeIndex++
    a.totalWrites--

    return true
}

func (a *SeqAgent) sendRead() bool {

    if a.readIndex >= len(a.readTrace) {
        return false
    }

    addr := a.readTrace[a.readIndex]

    req := mem.ReadReqBuilder{}.
        WithSrc(a.memPort.AsRemote()).
        WithDst(a.LowModule.AsRemote()).
        WithAddress(addr).
        WithByteSize(8).
        WithPID(1).
        Build()

    if err := a.memPort.Send(req); err != nil {
        return false
    }

    a.pendingReads[req.ID] = req


    a.readIndex++
    a.totalReads--

    return true
}