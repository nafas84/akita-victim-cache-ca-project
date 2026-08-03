package main

import (
	"encoding/binary"
	"fmt"

	"github.com/sarchlab/akita/v4/mem/cache/writeback"
	"github.com/sarchlab/akita/v4/mem/mem"
	"github.com/sarchlab/akita/v4/sim"
)

type CPUSpec struct {
	Freq sim.Freq
}

var DefaultCPUSpec = CPUSpec{Freq: 1 * sim.GHz}

type Access struct {
	IsWrite bool
	Addr    uint64
}

type TraceCPU struct {
	*sim.TickingComponent

	engine sim.Engine
    specCPU CPUSpec

	LowModule sim.Port
	memPort   sim.Port

	step int

	totalWrites int
	totalReads  int

	trace      []Access
	traceIndex int

	completedWrites int
	completedReads  int
	finished        bool

	pendingWrites map[string]*mem.WriteReq
	pendingReads  map[string]*mem.ReadReq

	// AMAT-P
	reqStartTimes      map[string]sim.VTimeInSec
	totalAccessLatency float64

	// AMAT-T
	L1Latency     float64
	VCLatency     float64
	MemoryLatency float64

	Cache       *writeback.Comp
	VictimCache *VictimCache
}

func NewTraceCPU(engine sim.Engine, specCPU CPUSpec, large bool, l1Lat, vcLat, memLat float64) *TraceCPU {

	a := &TraceCPU{
		engine:        engine,
        specCPU:       specCPU,
		L1Latency:     l1Lat,
		VCLatency:     vcLat,
		MemoryLatency: memLat,
		
		pendingWrites: make(map[string]*mem.WriteReq),
		pendingReads:  make(map[string]*mem.ReadReq),
		reqStartTimes: make(map[string]sim.VTimeInSec),
	}
    
    smallTrace := []Access{
		// Fill cache Write (16 misses)
		{true, 0x0000}, // B0 L1
		{true, 0x0040}, // B1
		{true, 0x0080}, // B2
		{true, 0x00C0}, // B3
		{true, 0x0100}, // B4
		{true, 0x0140}, // ...
		{true, 0x0180},
		{true, 0x01C0},

		{true, 0x0200},
		{true, 0x0240},
		{true, 0x0280},
		{true, 0x02C0},
		{true, 0x0300},
		{true, 0x0340},
		{true, 0x0380}, // B14
		{true, 0x03C0}, // B15

		// Read them back (6 hits)
		{false, 0x0000},
		{false, 0x0040},
		{false, 0x0080},
		{false, 0x00C0},
		{false, 0x0100},
		{false, 0x0140},

		// Evict B0 L1 (0x0000)
		{true, 0x0400},
		{false, 0x0000},

		// Evict B0 L1 (0x0000)
		{true, 0x0400},
		{false, 0x0000},

		// Evict B1 L1 (0x0040)
		{true, 0x0440},
		{false, 0x0040},

		// Evict B2 L1 (0x0080)
		{true, 0x0480},
		{false, 0x0080},

		// Evict B3 L1 (0x00C0)
		{true, 0x04C0},
		{false, 0x00C0},

		// Evict B4 L1 (0x0100)
		{true, 0x0500},
		{false, 0x0100},

		// Read and Write B6, B7, B8, B9 (6 hits)
		{false, 0x0180},
		{true, 0x01C0},
		{false, 0x01C0},
		{true, 0x0200},
		{false, 0x0200},
		{false, 0x0240},
	}

	largeTrace := []Access{}

	// Fill cache
	for i := 0; i < 16; i++ {
		largeTrace = append(largeTrace, Access{true, uint64(i * 64)})
	}

	// Repeat many phases
	for r := 0; r < 30; r++ {
		largeTrace = append(largeTrace, Access{true, 0x0400}, Access{false, 0x0000})
		for i := 0; i < 10; i++ {
			largeTrace = append(largeTrace,
				Access{true, 0x0000}, Access{false, 0x0400},
				Access{true, 0x0440}, Access{false, 0x0040},
			)
		}

		for i := 0; i < 10; i++ {
			largeTrace = append(largeTrace,
				Access{true, 0x0480}, Access{false, 0x0080},
				Access{true, 0x04C0}, Access{false, 0x00C0},
			)
		}
		//largeTrace = append(largeTrace,
		//	Access{false, 0x0140}, Access{true, 0x0180},
		//	Access{false, 0x01C0}, Access{true, 0x0200},
		//	Access{false, 0x0240}, Access{true, 0x0280},
		//	Access{false, 0x02C0}, Access{true, 0x0300},
		//)
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

	a.TickingComponent = sim.NewTickingComponent("TraceCPU", engine, 1*sim.GHz, a)
	a.memPort = sim.NewPort(a, 4, 4, "Mem")
	a.AddPort("Mem", a.memPort)

	return a
}

func (a *TraceCPU) Tick() bool {
	if a.finished {
		return false
	}

	progress := false

	progress = a.processResponses() || progress

	if !a.hasPendingRequest() && a.traceIndex < len(a.trace) {
		op := a.trace[a.traceIndex]
		if op.IsWrite {
			progress = a.sendWrite(op.Addr) || progress
		} else {
			progress = a.sendRead(op.Addr) || progress
		}
		a.traceIndex++
	}

	if a.totalWrites == 0 && a.totalReads == 0 && !a.hasPendingRequest() {

		fmt.Printf("Writes         : %d\n", a.completedWrites)
		fmt.Printf("Reads          : %d\n", a.completedReads)
		fmt.Println()

		fmt.Println("L1 Cache")
		fmt.Printf("  Read Hits    : %d\n", a.Cache.ReadHit)
		fmt.Printf("  Read Misses  : %d\n", a.Cache.ReadMiss)
		fmt.Printf("  Write Hits   : %d\n", a.Cache.WriteHit)
		fmt.Printf("  Write Misses : %d\n", a.Cache.WriteMiss)
		fmt.Printf("  WriteBacks   : %d\n", a.Cache.WriteBack)

		totalReq := a.completedReads + a.completedWrites
		if totalReq > 0 {
			l1MissRate := float64(a.Cache.ReadMiss+a.Cache.WriteMiss) / float64(totalReq)
			fmt.Printf("  Hit Rate     : %.2f%%\n", 100*(1-l1MissRate))
		}

		fmt.Println()

		var memTraffic uint64

		if a.VictimCache != nil {
			vcTotal := a.VictimCache.VCHits + a.VictimCache.VCMisses
			vcMisses := a.VictimCache.VCMisses

			fmt.Println("Victim Cache")
			fmt.Printf("  Hits         : %d\n", a.VictimCache.VCHits)
			fmt.Printf("  Misses       : %d\n", vcMisses)

			if vcTotal > 0 {
				fmt.Printf("  Hit Rate     : %.2f%%\n", 100*float64(a.VictimCache.VCHits)/float64(vcTotal))
			}

			memTraffic = a.VictimCache.BottomSendCount
            //memTraffic = a.VictimCache.VCMisses

		} else {
			fmt.Println("Victim Cache")
			fmt.Println("  Hits         : N/A")
			fmt.Println("  Misses       : N/A")
			fmt.Println("  Hit Rate     : N/A")

			memTraffic = a.Cache.ReadMiss + a.Cache.WriteMiss
		}

		fmt.Println()

        // Memory Traffic
		fmt.Printf("Memory Traffic : %d requests\n", memTraffic)

		// AMAT
		if totalReq > 0 {
			// Teorical
			l1MissRate := float64(a.Cache.ReadMiss+a.Cache.WriteMiss) / float64(totalReq)
			if a.VictimCache != nil {
				vcTotal := a.VictimCache.VCHits + a.VictimCache.VCMisses
				var vcHitRate, vcMissRate float64
				if vcTotal > 0 {
					vcHitRate = float64(a.VictimCache.VCHits) / float64(vcTotal)
					vcMissRate = float64(a.VictimCache.VCMisses) / float64(vcTotal)
				}

				TheoreticalAMAT := a.L1Latency + l1MissRate*(vcHitRate*a.VCLatency+vcMissRate*a.MemoryLatency)
				fmt.Printf("Theoretical AMAT : %.2f cycles\n", TheoreticalAMAT)

			} else {
				TheoreticalAMAT := a.L1Latency + l1MissRate*a.MemoryLatency
				fmt.Printf("Theoretical AMAT : %.2f cycles\n", TheoreticalAMAT)
			}

			// Practical
			SimulatedAMAT := a.totalAccessLatency / float64(totalReq)
			fmt.Printf("Simulated AMAT   : %.2f cycles\n", SimulatedAMAT)
		}

		// Execution Time
		execTimeSec := float64(a.engine.CurrentTime())
        freqInHz := float64(a.specCPU.Freq)
		execCycles := execTimeSec * freqInHz
		
        // Execution Time
		fmt.Printf("Execution Time : %.0f cycles (%.9f s)\n", execCycles, execTimeSec)
		fmt.Println("===================================")

		a.finished = true
	}

	return progress
}

func (a *TraceCPU) processResponses() bool {
	msg := a.memPort.RetrieveIncoming()
	if msg == nil {
		return false
	}

	switch rsp := msg.(type) {

	case *mem.WriteDoneRsp:
		// Write
		startTime := a.reqStartTimes[rsp.RespondTo]
		latencySec := a.engine.CurrentTime() - startTime
		a.totalAccessLatency += float64(latencySec) * float64(a.specCPU.Freq)

		delete(a.pendingWrites, rsp.RespondTo)
		delete(a.reqStartTimes, rsp.RespondTo)
		a.completedWrites++

	case *mem.DataReadyRsp:
		// Read
		startTime := a.reqStartTimes[rsp.RespondTo]
		latencySec := a.engine.CurrentTime() - startTime
		a.totalAccessLatency += float64(latencySec) * float64(a.specCPU.Freq)

		delete(a.pendingReads, rsp.RespondTo)
		delete(a.reqStartTimes, rsp.RespondTo)
		a.completedReads++
	}

	return true
}

func (a *TraceCPU) hasPendingRequest() bool {
	return len(a.pendingWrites) > 0 || len(a.pendingReads) > 0
}

func (a *TraceCPU) sendWrite(addr uint64) bool {
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
	a.reqStartTimes[req.ID] = a.engine.CurrentTime()
	a.totalWrites--

	return true
}

func (a *TraceCPU) sendRead(addr uint64) bool {
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
	a.reqStartTimes[req.ID] = a.engine.CurrentTime()
	a.totalReads--

	return true
}