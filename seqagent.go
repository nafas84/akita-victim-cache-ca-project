package main

import (
	"fmt"
	"encoding/binary"

	"github.com/sarchlab/akita/v4/mem/cache/writeback"
	"github.com/sarchlab/akita/v4/mem/mem"
	"github.com/sarchlab/akita/v4/sim"
)

const (
    HitTime     = 1.0  // cycles
    MissPenalty = 100.0 // cycles
)

type Access struct {
    IsWrite bool
    Addr    uint64
}

type SeqAgent struct {
	*sim.TickingComponent

	LowModule sim.Port

	memPort sim.Port

	step int

	totalWrites int
	totalReads  int

	trace []Access
	traceIndex int

	completedWrites int
	completedReads  int
	finished        bool

	pendingWrites map[string]*mem.WriteReq
	pendingReads  map[string]*mem.ReadReq	

	Cache *writeback.Comp
}

func NewSeqAgent(engine sim.Engine, large bool) *SeqAgent {

	a := &SeqAgent{
		pendingWrites: make(map[string]*mem.WriteReq),
		pendingReads:  make(map[string]*mem.ReadReq),
	}

	smallTrace := []Access{
    // Fill cache (16 writes)
    {true, 0x0000},
    {true, 0x0040},
    {true, 0x0080},
    {true, 0x00C0},
    {true, 0x0100},
    {true, 0x0140},
    {true, 0x0180},
    {true, 0x01C0},
    {true, 0x0200},
    {true, 0x0240},
    {true, 0x0280},
    {true, 0x02C0},
    {true, 0x0300},
    {true, 0x0340},
    {true, 0x0380},
    {true, 0x03C0},

    // Read them back (mostly hits)
    {false, 0x0000},
    {false, 0x0040},
    {false, 0x0080},
    {false, 0x00C0},
    {false, 0x0100},
    {false, 0x0140},

    // Capacity miss
    {true, 0x0400},
    {false, 0x0000},

    // Conflict thrashing
    {true, 0x0800},
    {false, 0x0000},
    {true, 0x0C00},
    {false, 0x0000},
    {true, 0x1000},
    {false, 0x0000},
    {true, 0x1400},
    {false, 0x0000},

    // Good locality again
    {false, 0x0180},
    {true, 0x01C0},
    {false, 0x01C0},
    {true, 0x0200},
    {false, 0x0200},
    {false, 0x0240},
}

	largeTrace := []Access{}

	// -------- Fill cache --------
	for i := 0; i < 16; i++ {
		largeTrace = append(largeTrace,
			Access{true, uint64(i * 64)},
		)
	}

	// -------- Repeat many phases --------
	for r := 0; r < 30; r++ {

		// Capacity miss
		largeTrace = append(largeTrace,
			Access{true, 0x0400},
			Access{false, 0x0000},
		)

		// Conflict thrashing
		for i := 0; i < 10; i++ {
			largeTrace = append(largeTrace,
				Access{true, 0x0000},
				Access{false, 0x0400},
				Access{true, 0x0800},
				Access{false, 0x0C00},
			)
		}

		// Good locality (hits)
		largeTrace = append(largeTrace,
			Access{false, 0x0140},
			Access{true,  0x0180},
			Access{false, 0x01C0},
			Access{true,  0x0200},
			Access{false, 0x0240},
			Access{true,  0x0280},
			Access{false, 0x02C0},
			Access{true,  0x0300},
		)
	}

	if large {
		a.trace = largeTrace
	} else {
		a.trace = smallTrace
	}

	for _, op := range a.trace {
		if op.IsWrite {
			a.totalWrites++
		} else {
			a.totalReads++
		}
	}

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
	if !a.hasPendingRequest() && a.traceIndex < len(a.trace) {
		op := a.trace[a.traceIndex]

		if op.IsWrite {
			progress = a.sendWrite(op.Addr) || progress
		} else {
			progress = a.sendRead(op.Addr) || progress
		}

		a.traceIndex++
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

		readTotal := a.Cache.ReadHit + a.Cache.ReadMiss
		if readTotal > 0 {
			readMissRate := float64(a.Cache.ReadMiss) / float64(readTotal)
			readAMAT := HitTime + readMissRate*MissPenalty

			fmt.Printf("Read AMAT    : %.2f cycles\n", readAMAT)
		}	


		writeTotal := a.Cache.WriteHit + a.Cache.WriteMiss
		if writeTotal > 0 {
			writeMissRate := float64(a.Cache.WriteMiss) / float64(writeTotal)
			writeAMAT := HitTime + writeMissRate*MissPenalty

			fmt.Printf("Write AMAT   : %.2f cycles\n", writeAMAT)
		}

		totalAccesses := readTotal + writeTotal
		totalMisses := a.Cache.ReadMiss + a.Cache.WriteMiss

		if totalAccesses > 0 {
			missRate := float64(totalMisses) / float64(totalAccesses)
			amat := HitTime + missRate*MissPenalty

			fmt.Printf("Overall AMAT : %.2f cycles\n", amat)
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


func (a *SeqAgent) sendWrite(addr uint64) bool {


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

    a.totalWrites--

    return true
}

func (a *SeqAgent) sendRead(addr uint64) bool {

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

    a.totalReads--

    return true
}