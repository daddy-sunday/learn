# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
# Compile the project
mvn compile

# Compile test code
mvn test-compile

# Run all tests
mvn test

# Run a single test class
mvn test -Dtest=ByteUtilTest
mvn test -Dtest=RaftCoreTest

# Run tests from ZMClientTest (integration tests, requires running nodes)
mvn test -Dtest=ZMClientTest
```

## Running the Raft Cluster

The project implements a Raft-based key-value store. To run a cluster:

1. Execute test methods in `src/test/java/com/zhiyuan/zm/RaftServiceTest.java` - contains 5 methods, each starts a Raft node
2. Default configuration is for 5 nodes; modify `GlobalConfig.java` for 3 nodes
3. After starting nodes, use `ZMClientTest` for CRUD operations and leader migration tests
4. Use `SaveLogTest.findAll()` to query RocksDB data from all nodes
5. Use `SaveLogTest.deleteAll()` to clear RocksDB data from all nodes

## High-Level Architecture

### Core Modules

```
src/main/java/com/zhiyuan/zm/
├── conf/                    # Configuration (GlobalConfig)
├── raft/
│   ├── constant/            # Constants (StatusCode, ServiceStatus, MessageType, DataOperationType)
│   ├── dto/                 # Data Transfer Objects
│   ├── exception/           # Custom exceptions (RaftFatalException, RaftRuntimeException)
│   ├── persistence/         # RocksDB wrappers (SaveData, SaveLog)
│   ├── role/                # Raft role implementations
│   │   ├── LeaderRole      # Leader state - handles writes, heartbeats, log replication
│   │   ├── FollowRole      # Follower state - receives heartbeats, votes
│   │   ├── CandidateRole   # Candidate state - requests votes
│   │   ├── BaseRole        # Common functionality for all roles
│   │   ├── RoleStatus      # Role state machine
│   │   ├── active/         # Background tasks (SyncLogTask, ApplyLogTask, ChaseAfterLogTask)
│   │   └── transaction/    # Distributed transaction support (TransactionService)
│   ├── rpc/                 # RPC communication (Bolt framework)
│   ├── service/             # Core services
│   │   ├── RaftService     # Main service entry point
│   │   ├── RaftStatus      # Cluster state (term, commitIndex, members)
│   │   └── RoleService     # Role management and request routing
│   └── util/                # Utilities (ByteUtil, KeyUtil)
└── extend/                  # Extension points (UserWork)
```

### Raft Protocol Flow

**Write Operation (Leader):**
1. Client request arrives at Leader
2. Entry written to log storage queue, sync queue, and apply queue
3. Wait for log replication to majority
4. Respond to client

**Follower receives Leader's log:**
1. Validate Raft message (term, logIndex)
2. Append entry to local log storage
3. Add to apply queue if committed
4. Respond to Leader

**Background Threads (per node):**
- `SaveLogTask`: Batch writes logs to RocksDB
- `SyncLogTask`: Leader replicates logs to followers
- `ApplyLogTask`: Applies committed logs to state machine
- `ChaseAfterLogTask`: Helps followers catch up on missed logs

### Key Design Decisions

1. **Role-based State Machine**: Each node runs in a loop, switching behavior based on `RoleStatus` (LEADER=1, FOLLOWER=2, CANDIDATE=3, LEARNER=4)

2. **Async Batch Processing**: Three asynchronous threads handle log persistence, replication, and application - improves throughput via batching

3. **RocksDB Storage**:
   - Logs stored with prefix: `LOG_KEY_PREFIX(10) + groupId(4) + logIndex(8)`
   - Data stored with prefix: `DATA_KEY_PREFIX(20) + groupId(4)`
   - Transaction IDs cached and periodically persisted

4. **Leader Migration**: Supports explicit leader transfer via `leaderMove(targetAddress)` - current leader stops accepting writes, syncs state, then notifies target to become leader

5. **Exception Handling**: Uses `RaftFatalException` for unrecoverable errors instead of `System.exit()` - allows graceful shutdown

### Important Classes

| Class | Responsibility |
|-------|----------------|
| `RaftService` | Main entry point, initializes persistence, RPC server, and starts `RoleService` |
| `RoleService` | Routes requests to current role, manages role transitions |
| `LeaderRole` | Handles client writes, heartbeats, log replication, transaction management |
| `BaseRole` | Common log handling for Leader/Follower |
| `TransactionService` | Distributed transaction support with ID generation and status tracking |
| `RaftStatus` | Cluster state: term, votedFor, commitIndex, appliedIndex, members |

### Data Structures

- `LogEntries`: Raft log entry (logIndex, term, message)
- `AddLogRequest`: AppendEntries RPC payload
- `VoteRequest`: RequestVote RPC payload
- `Row`: Key-value pair for client data
- `Command`: Batch operations (INSERT/DELETE/UPDATE)

## Known Limitations / TODO

1. **Snapshot not implemented**: Log compaction via RocksDB snapshots is planned but not implemented
2. **Configuration change**: Dynamic membership changes (add/remove nodes) not yet implemented
3. **Transaction cleanup**: Garbage collection for rolled-back transactions needs implementation
4. **Multi-Raft**: Currently single Raft group; multi-Raft for sharding is planned

## Recent Improvements (2026-03-03)

- Fixed all `System.exit(100)` calls → replaced with `RaftFatalException`
- Fixed typos: `appliedInedex`→`appliedIndex`, `catchNumber`→`cacheNumber`, etc.
- Fixed `KeyUtil.generateCommon()` ByteBuffer size bug (5→9 bytes for long keys)
- Improved log levels: DEBUG→INFO for important operations, added `isDebugEnabled()` checks
- Enhanced transaction persistence: transactions now recoverable after node restart
- Added 45 unit tests covering DTOs, utilities, and core classes
