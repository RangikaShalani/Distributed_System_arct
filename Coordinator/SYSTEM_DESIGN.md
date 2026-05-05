# Distributed Log Processing System Design

## 1. System Overview

This service is a small HTTP-based distributed log processing cluster. Every node runs the same Node.js application and can dynamically become one of these runtime roles:

- `COORDINATOR`
- `AGGREGATOR`
- `VALIDATOR`
- `MAPPER`
- `UNASSIGNED`

The system has two major responsibilities:

1. Maintain a live cluster with leader election, membership tracking, role assignment, and failover.
2. Process a selected log file by splitting it across mapper nodes, validating mapper output, and aggregating the final summary.

Each node also writes its runtime logs to a per-port file under `sidecar_logs/`.

## 2. Runtime Architecture

### Main entry point

`app.js` starts an Express server, enables JSON request parsing, initializes node logging, attaches request/response logging middleware, starts cluster management, and starts the heartbeat loop.

### Core modules

- `core/clusterManager.js`
  Handles cluster bootstrap, joining, leader election, membership snapshots, role rebalancing, failover, and job dispatch.

- `core/heartbeat.js`
  Runs periodic health checks between the Coordinator and followers.

- `roles/mapper.js`
  Reads the assigned log slice, summarizes it, requests validator confirmation, and forwards accepted results to the Aggregator.

- `roles/validator.js`
  Recomputes the mapper slice summary and returns an accept or reject vote.

- `roles/aggregator.js`
  Merges validated mapper outputs into the final per-job totals.

- `sidecar/proxy.js`
  Wraps outbound `POST` requests with sidecar-style logging and a single retry on failure.

- `utils/logger.js`
  Redirects console output into `sidecar_logs/<port>.log`, and keeps HTTP request traces file-only to avoid terminal noise.

## 3. Cluster State Model

The Coordinator state module keeps a shared in-memory snapshot with the following important fields:

- `selfPort`
- `selfId`
- `role`
- `leaderId`
- `aggregatorId`
- `electionTerm`
- `version`
- `nodes`
- `lastLeaderHeartbeat`
- `filePath`

### Node identity

Each node ID is derived from its port using the format:

```text
localhost:<port>
```

Example:

```text
localhost:8001
```

### Membership record

Each entry in `nodes` contains:

- `id`
- `port`
- `role`
- `status`
- `lastSeen`

`status` is currently tracked as either alive or dead.

## 4. Cluster Bootstrap And Joining

### Startup flow

When a node starts:

1. It initializes itself as `UNASSIGNED`.
2. It inserts its own membership record locally.
3. After a short delay, it probes peer ports using `GET /cluster/status`.
4. It chooses the newest discovered snapshot using:
   - higher `electionTerm` first
   - higher port as tie-breaker
5. It then follows one of these paths:
   - if a live leader exists, join that leader through `POST /cluster/join`
   - if peers exist but no live leader is known, begin election
   - if no suitable peers exist, become leader immediately

### Seed port discovery

Peer discovery uses `CLUSTER_PORTS` when present. If that environment variable is not set, the node scans ports `8000` through `8020`.

### Join flow

When a follower joins the current leader:

1. It sends `port` and `nodeId` to `POST /cluster/join`.
2. The leader inserts the node into membership.
3. The leader increments the cluster `version`.
4. The leader recalculates runtime roles for all live non-leader nodes.
5. The leader broadcasts the updated snapshot through `POST /cluster/sync`.
6. The join response returns the assigned role and the latest cluster snapshot.

## 5. Role Assignment Strategy

Role assignment is recalculated from scratch whenever the Coordinator rebalances the cluster.

### Coordinator selection

The current leader always has role `COORDINATOR`.

### Aggregator selection

Among live non-leader nodes, the lowest port is chosen as `AGGREGATOR`.

### Worker role balancing

All remaining live non-leader, non-aggregator nodes are assigned roles in ascending port order.

The system enforces this worker shape:

1. Create at least 2 `VALIDATOR` nodes.
2. Create at least 1 `MAPPER`.
3. After that:
   - if `mapperCount <= validatorCount`, assign `MAPPER`
   - otherwise assign `VALIDATOR`

This produces a cluster that always prefers enough validators first, then grows mapper capacity while keeping validators available for quorum checks.

### Minimum useful cluster

To successfully run `/start`, the cluster must have at least:

- 1 `COORDINATOR`
- 1 `AGGREGATOR`
- 2 `VALIDATOR`
- 1 `MAPPER`

That means at least 5 live nodes are needed for job execution.

## 6. Leader Election And Failover

### Election model

The system uses a bully-style election based on port number. Higher port means higher priority.

### Election trigger conditions

An election begins when:

- bootstrap finds no active leader
- a follower detects leader heartbeat failure
- a node receives an election request from a lower-priority candidate and decides to take over the election

### Election flow

1. The candidate increments `electionTerm`.
2. It clears its current `leaderId` and sends `POST /cluster/election` to all live peer nodes.
3. Each receiver compares the incoming `candidateId` against its own port:
   - if receiver port is higher than the candidate port, it replies with `ok: true`
   - if receiver port is lower than or equal to the candidate port, it does not send an election OK response
4. If no higher node acknowledges, the candidate becomes the Coordinator.
5. If any higher node acknowledges, the candidate waits for a leader announcement.
6. If no leader appears before timeout, the candidate retries election.

### Becoming leader

When a node becomes leader:

1. It sets `leaderId = selfId`.
2. It sets its role to `COORDINATOR`.
3. It increments `electionTerm`.
4. It rebalances roles.
5. It increments the cluster `version`.
6. It announces leadership through `POST /cluster/coordinator` to all peers in parallel.
7. It broadcasts the latest snapshot through `POST /cluster/sync` to all peers in parallel.
8. Any node that learns a new `leaderId` logs the message `${port} is the leader`.

### Aggregator failover

If the Coordinator marks the current Aggregator dead:

1. It selects the lowest-port remaining live non-leader node as the new Aggregator.
2. It rebalances the rest of the worker roles.
3. It increments the version and rebroadcasts the snapshot.

This keeps future mapper outputs pointed at the correct Aggregator.

## 7. Heartbeat And Failure Detection

### Heartbeat timing

- Heartbeat interval: 3000 ms
- Follower leader-timeout threshold: 7000 ms
- HTTP timeout for heartbeat requests: 1000 ms

### Coordinator heartbeat behavior

The Coordinator sends `GET /heartbeat` to every other known node. On success, it refreshes that node's `lastSeen`, role, and status. On failure, it marks the node dead.

### Follower heartbeat behavior

Each follower sends `GET /heartbeat` only to the current Coordinator. On success, it updates:

- `lastLeaderHeartbeat`
- leader identity
- leader liveness

If the heartbeat fails, or the local leader heartbeat timestamp becomes too old, the follower starts leader failover.

### Dead node handling

When a node is marked dead:

- its membership status becomes `dead`
- the event is logged
- if it was the Aggregator, the Coordinator reassigns the Aggregator
- if it was the leader and the local node is a follower, leader failover starts
- if the local node is the Coordinator, roles are rebalanced and a fresh snapshot is broadcast

## 8. Job Execution Workflow

### Job entry point

Jobs are started with `GET /start`, but only the Coordinator accepts that request.

If a follower receives `/start`, it returns `403 Not coordinator`.

### File selection

The Coordinator does not receive the file path through HTTP. Instead, it prompts in the terminal:

```text
Enter log file path for this job:
```

The selected path is validated to ensure:

- it is not empty
- it exists
- it is a file

### Dispatch preparation

Before dispatch:

1. The Coordinator rebalances roles one more time.
2. It loads the selected file into memory.
3. It splits the file into non-empty lines.
4. It computes `chunkSize = ceil(totalLines / mapperCount)`.
5. It creates a unique `jobId` using the current timestamp.

### Mapper dispatch model

Each mapper receives metadata for one contiguous line range:

- `jobId`
- `chunkId`
- `startLine`
- `endLine`
- `sourceFilePath`
- `validators`
- `aggregator`

The Coordinator currently sends `sourceFilePath` and line boundaries instead of sending the entire chunk body. This means workers reconstruct their chunk locally from the same shared file path.

### Validator selection

Each mapper is assigned up to 2 validators using a rotating selection across the validator list:

- validator at `mapperIndex`
- validator at `mapperIndex + 1`

wrapped modulo the validator count

Because `/start` already requires at least 2 validators, the current workflow normally uses exactly 2 validator targets per mapper request.

## 9. Mapper Logic

### Chunk materialization

The mapper reconstructs its working slice using `readChunk()`:

- if `chunk` is already present in the request body, use it
- otherwise read `sourceFilePath`
- split the file into non-empty lines
- slice from `startLine` to `endLine`

### Summarization algorithm

For each line in the chunk:

1. Parse severity using substring matching.
2. Increment the counter for that severity.

Current severity mapping:

- contains `FATAL` -> `CRITICAL`
- contains `ERROR` -> `ERROR`
- contains `WARNING` -> `WARNING`
- contains `INFO` -> `INFO`
- contains `DEBUG` -> `DEBUG`
- otherwise -> `INFO`

If the input line itself is missing or empty, the parser returns `UNKNOWN`, though empty lines are normally filtered out before processing.

### Current summary format

Mapper summaries currently contain counts only:

```json
{
  "ERROR": { "count": 12 },
  "INFO": { "count": 30 }
}
```

The older unique-message collection logic is no longer active in `summarizer.js`.

### Validation fan-out

The mapper sends validation requests concurrently with `Promise.allSettled()` so one validator failure does not prevent collecting other responses.

### Quorum rule

Quorum is computed as:

```text
floor(numberOfValidators / 2) + 1
```

With the current normal 2-validator assignment, both validators must accept.

### Success path

If quorum is reached:

1. The mapper logs the quorum result.
2. It sends the accepted summary to `POST /aggregate`.
3. It returns:

```json
{
  "status": "validated",
  "chunkId": "...",
  "accepted": 2,
  "quorum": 2
}
```

### Failure path

If quorum is not reached:

1. The mapper does not contact the Aggregator.
2. It returns HTTP `409`.
3. The response includes the collected validator votes.

## 10. Validator Logic

Each validator independently reconstructs the same chunk using `sourceFilePath`, `startLine`, and `endLine`.

It then:

1. recomputes the chunk summary
2. compares it with the mapper result using `JSON.stringify(...)`
3. returns a vote with `accepted: true` when the summaries match exactly

Validator response format:

```json
{
  "jobId": "...",
  "chunkId": "...",
  "accepted": true,
  "validatorAt": 1777895692874,
  "reason": "MATCH"
}
```

Possible `reason` values:

- `MATCH`
- `MISMATCH`

## 11. Aggregation Logic

The Aggregator stores in-memory state per `jobId`.

For each job it keeps:

- `processedChunks`
- `totals`
- `uniqueMessages`

### Duplicate handling

If the same `chunkId` arrives more than once, the Aggregator returns `"Duplicate"` and does not merge it again.

### Merge behavior

For each severity in the mapper result:

1. initialize the severity bucket if missing
2. add `summary.count` into `totals[severity]`
3. merge `summary.messages` into `uniqueMessages[severity]` when present

Because mapper summaries currently contain only counts, `uniqueMessages` remains allocated but stays empty in normal execution.

### Final output format

After each accepted chunk merge, the Aggregator logs and returns an output array like:

```json
[
  { "severity": "ERROR", "count": 12, "uniqueMessages": 0 },
  { "severity": "INFO", "count": 30, "uniqueMessages": 0 }
]
```

There is no explicit job-complete signal yet. The Aggregator simply updates and logs the running final result whenever a validated chunk arrives.

## 12. Communication Model

All node-to-node communication is plain HTTP on `localhost`.

### Cluster control endpoints

- `POST /register`
- `POST /cluster/join`
- `GET /cluster/status`
- `POST /cluster/sync`
- `POST /cluster/election`
- `POST /cluster/coordinator`
- `GET /heartbeat`

`POST /register` and `POST /cluster/join` are both routed to the same registration handler.

### Job execution endpoints

- `GET /start`
- `POST /map`
- `POST /validate`
- `POST /aggregate`

### Sidecar request behavior

Outbound `GET` and `POST` calls made through `sidecar/proxy.js`:

- log request metadata to the node log file
- attempt the request once
- retry once if the first attempt fails
- record success, failure, retry, and circuit-breaker metrics
- attach or propagate `x-request-id` headers for tracing

## 13. Logging Behavior

Each node initializes a dedicated log file:

```text
sidecar_logs/<port>.log
```

### Console interception

`initNodeLogger()` wraps:

- `console.log`
- `console.info`
- `console.warn`
- `console.error`

and writes them into the node's log file with timestamps and levels.

### HTTP request logging

The Express middleware logs:

- inbound request method, URL, and body
- outbound response status, duration, and body

These request/response trace lines go to the log file only and are not echoed to the terminal.

### Error logging

Unhandled exceptions and unhandled promise rejections are also written to the node log file.

## 14. Current Constraints And Tradeoffs

- Cluster state is in-memory only. Restarting a node loses its local runtime state until it rejoins or rebuilds from peers.
- Workers assume the same `sourceFilePath` is readable from every participating node. This works for a shared local environment, but not for truly isolated machines.
- File chunks are re-read from disk on mapper and validator nodes, which is simple but not optimized.
- Aggregation state is in-memory only and has no persistence or explicit completion lifecycle.
- Validation checks only count summaries. Unique-message validation is not active in the current code path.
- There is no mid-job reassignment or checkpointing if a mapper dies during an active job.

## 15. End-To-End Workflow Summary

1. Nodes start and probe peers.
2. One node becomes Coordinator or joins an existing Coordinator.
3. The Coordinator assigns roles and broadcasts the cluster snapshot.
4. Heartbeats keep membership fresh and trigger failover when needed.
5. The operator calls `GET /start` on the Coordinator.
6. The Coordinator asks for a local log file path in the terminal.
7. The Coordinator splits that file into line ranges and dispatches mapper jobs.
8. Each mapper rebuilds its chunk, summarizes it, and asks validators to verify it.
9. If quorum is reached, the mapper forwards the summary to the Aggregator.
10. The Aggregator merges accepted chunk totals and logs the running final result.
