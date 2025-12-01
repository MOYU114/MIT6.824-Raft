MIT 6.5840 Distributed Systems Labs
===================================

Overview
--------
This repository contains my solutions for the MIT 6.5840 (formerly 6.824) distributed systems labs.  Each lab builds a layer of a replicated key/value store stack on top of a Raft consensus core, exercising fault-tolerance, replication, and sharding techniques.  The Code lives under `src`, matching the Go module declared in `src/go.mod`.

Repository Layout
-----------------
- `src/raft`: Stand-alone Raft implementation used by the higher-level services.
- `src/kvraft`: Linearizable key/value store powered by Raft, including snapshot support.
- `src/kvsrv`: Single-server key/value reference implementation used early in the course.
- `src/mr` & `src/mrapps`: MapReduce coordinator/worker runtime and workloads for Lab 1.
- `src/main`: Command-line harnesses for MapReduce and lock service examples.
- `src/shardctrler`, `src/shardkv`: Scaffolding for the sharded key/value and controller labs.
- `src/labrpc`, `src/labgob`, `src/models`, `src/porcupine`: Supporting RPC, serialization, data models, and linearizability checking utilities.

Raft Node Architecture
----------------------
- **Core structure (`src/raft/raft.go`)**: Each peer keeps persistent fields (`currentTerm`, `votedFor`, snapshot metadata, and the log itself) plus volatile state (`commitIndex`, `lastApplied`, role, leader hint) and leader-only arrays (`nextIndex`, `matchIndex`).  The constructor (`Make`) bootstraps these fields, restores persisted state, and launches the ticker, applier, and per-follower replication goroutines.
- **RPC interfaces**:  
  - `RequestVote` implements the election protocol with up-to-date log checks and term-based role transitions.  
  - `AppendEntries` covers both heartbeats and log replication, includes the conflict hint fields (`ConflictTerm`, `ConflictIndex`), and advances `commitIndex` before waking the applier.  
  - `InstallSnapshot` ships compacted state to lagging followers and is also invoked by state machines when they load a snapshot from disk.
- **State machine application**: The `ApplyMsg` struct carries either committed log entries (`CommandValid`) or snapshots (`SnapshotValid`).  A dedicated `applier` goroutine waits on `applierCond`, slices out the committed prefix, and pushes messages on `applyCh`.  Upper layers such as `src/kvraft/server.go` consume the stream, deduplicate commands, mutate local state, and optionally save a snapshot via `Raft.Snapshot`.

Optimizations and Innovations
-----------------------------
1. **Per-follower replicator goroutines**: `Make` spawns a goroutine and `sync.Cond` per follower (`replicatorCond`).  Each replicator waits until `nextIndex` lags the leader and then calls `replicateOneRound`, which decides between `AppendEntries` and `InstallSnapshot`.  This avoids the bottleneck of a single replication loop and keeps slow followers from delaying the leader’s progress.
2. **Fast log conflict rollback**: Followers return detailed conflict hints from `AppendEntries` (term/index pairs).  Leaders process them in `handleAppendEntriesReply`, jumping `nextIndex` directly to the right term boundary instead of decrementing one entry at a time.  Combined with term-aware `matchLog` checks, this drastically shortens recovery time after partitions.
3. **Log trimming and snapshotting**: Both the KV server (`saveSnapshot`, `readPersist`) and Raft (`Snapshot`, `InstallSnapshot`) trim replicated logs and persist compact snapshots once `Persister.RaftStateSize()` crosses `maxraftstate`.  Followers that lag behind the snapshot boundary automatically receive an `InstallSnapshot` before resuming incremental replication, enabling large-state workloads to stay within memory limits.
4. **Condition-variable scheduling and goroutine management**: Heartbeats and replication piggyback on timers instead of busy loops.  `applierCond` and each `replicatorCond` wake only the goroutines that have work to do, keeping CPU overhead low even when no client traffic arrives.  Carefully scoped goroutines (`ticker`, `applier`, per-peer replicators) respect the `rf.dead` flag to avoid leaks after tests finish.
5. **State-machine friendly API**: `Start` appends a new `LogEntry` and signals replicators immediately, while `GetState` provides the current term/leader hint so clients can minimize futile RPCs.  The KV server layer couples these APIs with per-command notification channels, deduplication (`lastApplies`), and snapshot hooks so user-visible operations remain linearizable even when Raft is compacting its log.

Running and Testing
-------------------
All commands run from the `src` directory:

```bash
cd src
# Raft-only tests
go test ./raft -run TestBasic

# Key/value service with snapshots
go test ./kvraft

# MapReduce coordinator/worker integration
go test ./mr
```

Use `go test -run` to execute individual cases, and set `GOMAXPROCS` if you want to stress concurrency.  Debug logging can be enabled inside each package (`Debug` constants) when diagnosing failures.
