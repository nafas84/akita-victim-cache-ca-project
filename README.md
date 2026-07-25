# Akita Victim Cache

This project implements and evaluates a **Victim Cache** architecture using the **Akita** simulator. The goal is to reduce **conflict misses** in a direct-mapped L1 cache by adding a small fully-associative victim cache between the L1 cache and L2 memory.

The project was developed as part of the **Computer Architecture** course. It compares the performance of a standard direct-mapped cache with a cache hierarchy enhanced by an 8-entry Victim Cache, measuring cache hits, misses, memory traffic, and execution behavior.

---

## Project Overview

The project contains a simple cache simulator written in Go with the following components:

- **L1 Cache** (Direct-Mapped)
- **Victim Cache** (8-entry Fully Associative)
- **L2 Memory Simulator**
- **Benchmark Program** for generating memory access patterns
- Performance counters for hit/miss analysis

The benchmark generates conflict-heavy memory accesses to demonstrate how the Victim Cache reduces unnecessary accesses to L2 memory.

---

## About Akita

This project is implemented using **Akita**, an event-driven hardware simulation framework written in Go.

Akita provides a modular environment for modeling computer architecture components and simulating communication between them. It allows developers to build and evaluate custom hardware designs while keeping components independent and reusable.

In this project, Akita is used as the simulation framework for implementing the cache hierarchy and evaluating the behavior of the Victim Cache architecture.

---

## Project Structure

```
akita-victim-cache/
├── go.mod
├── go.sum
├── main.go
├── l1cache.go
├── victimcache.go
└── l2memory.go
```

---

## Requirements

- Go 1.20 or later
- Akita framework

Install project dependencies:

```bash
go mod tidy
```

---

## Running the Project

Clone the repository:

```bash
git clone <repository-url>
cd akita-victim-cache
```

Run the simulation:

```bash
go run .
```

The program executes the benchmark and prints cache statistics, including:

- L1 Cache Hits
- L1 Cache Misses
- Victim Cache Hits
- Victim Cache Misses
- L2 Memory Accesses
- Performance comparison between the baseline and Victim Cache architectures

---

## Team Members

| Student Number | Name                   |
|----------------|------------------------|
| 403105714      | Arash Akbari           |
| 403105974      | Aynaz Rahmani          |
| 403106024      | Nafiseh Zarei           |
