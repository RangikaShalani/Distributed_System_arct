# Distributed Log Processing Architecture Report

## 1. Outline Of The Architectural Approach

The system uses a role-based distributed architecture where every node runs the same executable but can dynamically act as:

- `COORDINATOR`
- `AGGREGATOR`
- `VALIDATOR`
- `MAPPER`
- `UNASSIGNED`

The architectural approach is built around these ideas:

- leader-driven cluster management
- runtime role assignment instead of fixed node specialization
- line-range partitioning for workload distribution
- quorum-based validation before aggregation
- lightweight HTTP communication between all nodes
- per-node sidecar communication layer for observability and resilience

At a high level, the Coordinator manages cluster membership, elections, heartbeats, and job dispatch. Mappers process file slices, Validators independently verify mapper output, and the Aggregator merges only validated results.

This gives the system a simple but practical structure for distributed processing:

1. discover or elect a leader
2. assign worker roles dynamically
3. partition the log workload
4. validate mapper output through distributed voting
5. aggregate only quorum-approved results

## 2. Detailed Solution Design

### 2.1 Node Model

Each node is identified by:

```text
localhost:<port>
```

Each node stores runtime membership data in memory with:

- `id`
- `port`
- `role`
- `status`
- `lastSeen`

The cluster snapshot also tracks:

- `leaderId`
- `aggregatorId`
- `electionTerm`
- `version`

### 2.2 Cluster Formation

When a node starts:

1. it marks itself `UNASSIGNED`
2. it probes peers using `GET /cluster/status`
3. it attempts to locate a live leader
4. if a leader exists, it joins using `POST /cluster/join`
5. if peers exist but no live leader is known, it participates in a bully-style election
6. if no peer responds as a higher-priority candidate, it becomes Coordinator

### 2.3 Role Assignment

The Coordinator recalculates roles for all live non-leader nodes.

Assignment policy:

1. lowest live non-leader port becomes `AGGREGATOR`
2. remaining workers are assigned in ascending port order
3. create at least 2 `VALIDATOR` nodes first
4. create at least 1 `MAPPER`
5. after that:
   - assign `MAPPER` when `mapperCount <= validatorCount`
   - otherwise assign `VALIDATOR`

This guarantees a minimum useful topology before jobs are allowed to run.

### 2.4 Job Processing Design

Only the Coordinator accepts `GET /start`.

The Coordinator then:

1. prompts for a file path from the terminal
2. validates that the file exists
3. reads the file into memory
4. removes empty lines
5. computes `chunkSize = ceil(totalLines / mapperCount)`
6. creates contiguous line-range chunks
7. dispatches one chunk range per mapper

Each mapper request includes:

- `jobId`
- `chunkId`
- `startLine`
- `endLine`
- `sourceFilePath`
- `validators`
- `aggregator`

The current implementation sends file metadata rather than the raw chunk body. Mapper and validator nodes reconstruct the exact line slice locally from the same file path.

### 2.5 Processing Logic

Mapper logic:

- read its assigned slice
- summarize severity counts
- send validation requests to assigned validators
- wait for votes using `Promise.allSettled`
- forward result to Aggregator only if quorum is reached

Validator logic:

- rebuild the same chunk locally
- recompute the summary
- compare recomputed result with mapper result
- return `MATCH` or `MISMATCH`

Aggregator logic:

- track processed chunk IDs to prevent duplicates
- merge accepted totals into per-job aggregates
- keep a running final output for each job

## 3. Sidecar Implementation Details

Each node contains a dedicated sidecar layer implemented in:

- `utils/logger.js`
- `sidecar/proxy.js`

This sidecar is responsible for:

- encapsulating all inter-node communication
- logging inbound and outbound requests
- attaching request IDs
- recording timestamps and response time
- collecting communication metrics
- retrying transient failures
- opening circuits for repeatedly failing peers

### 3.1 Logger Design

Each node initializes its own log file:

```text
sidecar_logs/<port>.log
```

The logger wraps:

- `console.log`
- `console.info`
- `console.warn`
- `console.error`

All console messages are timestamped and written into the per-node log file.

### 3.2 HTTP Request Trace Logging

The Express middleware records:

- request timestamp
- request ID
- inbound request method and URL
- inbound request body
- outbound response status
- request duration
- outbound response body

The request/response traces are now written to the log file only, not to the terminal.

### 3.3 Proxy Behavior

All outbound internal `GET` and `POST` requests go through `sidecar/proxy.js`.

The proxy now provides:

1. a communication abstraction layer used by cluster join, sync, election, heartbeat, mapper, validator, and aggregator flows
2. generated or propagated `x-request-id` values for end-to-end tracing
3. timeout protection on outbound calls
4. bounded retry handling
5. per-target circuit breaker state
6. in-memory metrics for requests, success/failure counts, retries, and latency

Metrics are exposed from each node at:

```text
GET /sidecar/metrics
```

This allows each node to report:

- total requests
- successes and failures
- retry count
- open-circuit rejections
- average latency per target
- last error per target

### 3.4 Why This Sidecar Pattern Helps

This sidecar approach gives:

- per-node observability
- request tracing with request IDs across nodes
- consistent timestamps and response time logs
- centralized retry and circuit breaking policy
- easier debugging of inter-node communication
- low implementation complexity

## 4. Distributed Algorithm Explanation

### 4.1 Leader Election Algorithm

The system uses a bully-style election based on port number.

Rule:

- the higher port has higher election priority

Election steps:

1. a candidate increments `electionTerm`
2. it clears its current `leaderId`
3. it contacts all live peer nodes through `POST /cluster/election`
4. a peer replies `ok: true` only when its own port is higher than the candidate port
5. if no higher node responds, the candidate becomes Coordinator
6. if a higher node responds, that higher node is expected to continue the election
7. if no leader stabilizes within timeout, election is retried

This makes leader selection deterministic without a central registry.

### 4.2 Work Distribution Algorithm

The processing algorithm is static line-range partitioning per job.

For a file with `N` non-empty lines and `M` mappers:

```text
chunkSize = ceil(N / M)
```

Each mapper gets a contiguous block of lines.

Advantages:

- simple partitioning
- low scheduling overhead
- predictable mapper responsibilities
- naturally rebalanced on the next job when node count changes

### 4.3 Validation Algorithm

Each mapper sends its summary to 2 validators in the normal cluster configuration.

Each validator:

1. recomputes the result independently
2. compares with mapper output
3. returns a vote

Quorum rule:

```text
floor(numberOfValidators / 2) + 1
```

With 2 validators, both must accept.

This protects the Aggregator from accepting a faulty mapper result without independent confirmation.

### 4.4 Aggregation Algorithm

Only quorum-approved chunk results are sent to the Aggregator.

For each chunk:

1. ignore duplicate `chunkId`
2. merge severity counts into job totals
3. update running result

This makes aggregation idempotent at chunk level and avoids double counting from retries or duplicates.

## 5. Sequence Diagrams Of Communication Flow

### 5.1 Cluster Join Flow

```text
New Node -> Peer Nodes: GET /cluster/status
Peer Nodes -> New Node: snapshot responses
New Node -> Coordinator: POST /cluster/join
Coordinator -> Coordinator: update membership + rebalance roles
Coordinator -> All Nodes (parallel): POST /cluster/sync
Coordinator -> New Node: join response with role + snapshot
```

### 5.2 Leader Election Flow

```text
Follower -> Leader: GET /heartbeat
Leader X-> Follower: no response
Follower -> All Peer Nodes: POST /cluster/election
Higher Node -> Follower: ack / willRunElection
Higher Node -> Higher Peers: POST /cluster/election
Highest Live Node -> All Nodes (parallel): POST /cluster/coordinator
Highest Live Node -> All Nodes (parallel): POST /cluster/sync
All Nodes -> All Nodes: log "${port} is the leader" when leaderId changes
```

### 5.3 Job Execution Flow

```text
Operator -> Coordinator: GET /start
Coordinator -> Terminal: prompt for file path
Coordinator -> Mapper: POST /map
Mapper -> Validator A: POST /validate
Mapper -> Validator B: POST /validate
Validator A -> Mapper: accept/reject
Validator B -> Mapper: accept/reject
Mapper -> Mapper: quorum decision
Mapper -> Aggregator: POST /aggregate
Aggregator -> Mapper: aggregated response
Mapper -> Coordinator: validated/rejected response
```

### 5.4 Heartbeat Flow

```text
Coordinator -> Worker Nodes: GET /heartbeat
Worker Nodes -> Coordinator: liveness response

Follower -> Coordinator: GET /heartbeat
Coordinator -> Follower: liveness response
```

## 6. Failure Scenarios And Fault Tolerance Strategy

### 6.1 Coordinator Failure

Scenario:

- followers cannot reach the Coordinator through heartbeat

Strategy:

- trigger bully election
- highest live node becomes new Coordinator
- new Coordinator announces itself to all peers in parallel
- new Coordinator rebroadcasts cluster state to all peers in parallel

Result:

- control plane recovers automatically without manual intervention

### 6.2 Aggregator Failure

Scenario:

- Coordinator detects Aggregator heartbeat failure

Strategy:

- mark Aggregator dead
- choose a new Aggregator from live non-leader nodes
- rebalance remaining worker roles
- broadcast updated snapshot

Result:

- future chunk results are redirected to the new Aggregator

### 6.3 Validator Failure

Scenario:

- one or more validators become unavailable

Strategy:

- the Coordinator eventually marks them dead
- roles are rebalanced on the next cluster update
- `/start` is rejected if fewer than 2 validators are available

Result:

- the system prefers correctness over degraded validation

### 6.4 Mapper Failure

Scenario:

- a mapper fails before or during processing

Strategy:

- no mid-job reassignment currently exists
- the failed chunk is not automatically redistributed
- future jobs use the updated live mapper set after rebalance

Result:

- the current job may complete only partially
- the next job will rebalance cleanly

### 6.5 Network Or Request Failure

Scenario:

- internal HTTP call fails transiently

Strategy:

- `sidecar/proxy.js` retries transient internal calls
- repeated failures open a temporary circuit for that peer/route

Result:

- temporary glitches may recover automatically
- persistently failing routes are isolated quickly instead of being hammered continuously

### 6.6 Duplicate Delivery

Scenario:

- a mapper or retry sends the same chunk twice

Strategy:

- Aggregator stores `processedChunks`
- duplicate `chunkId` is ignored

Result:

- chunk-level aggregation is protected from double counting

## 7. Scalability Strategy

### 7.1 Horizontal Scaling Model

The cluster scales horizontally by adding more nodes. Because all nodes run the same binary, new nodes can join dynamically and receive roles automatically.

### 7.2 Mapper Scaling

As more workers are added, role balancing eventually increases mapper count. This improves throughput because the Coordinator partitions the file across more mapper line ranges.

### 7.3 Validator Scaling

The current strategy also grows validator capacity over time. This supports continued quorum-based verification as worker count increases.

### 7.4 Rebalancing Between Jobs

The system currently scales best across job boundaries:

- new workers affect the next dispatch
- dead workers are excluded from the next dispatch
- no live chunk migration is required

This keeps the coordination model simple and avoids expensive mid-job resharding logic.

### 7.5 Current Scalability Limits

The present code has several practical limits:

- cluster membership is held only in memory
- heartbeats are centralized through the Coordinator
- file content is reread locally by mappers and validators
- all communication is synchronous HTTP over localhost
- aggregation state is in memory only

This design is appropriate for a coursework-style or prototype distributed system, but would need persistence, remote-shared storage, and stronger scheduling for large-scale deployment.

## 8. Data Consistency Guarantees

### 8.1 Membership Consistency

Cluster membership is maintained through versioned snapshots with:

- `electionTerm`
- `version`

Nodes apply only newer snapshots. This gives eventual convergence toward the latest known Coordinator view.

### 8.2 Leader Consistency

At any moment during failover there may be a short transition period, but the bully election ensures the highest live priority node wins once the system stabilizes.

This provides eventual single-leader consistency.

Whenever a node observes a leader change through self-promotion, heartbeat, or snapshot synchronization, it logs `${port} is the leader`.

### 8.3 Processing Consistency

Chunk results are not aggregated immediately after mapping. They are aggregated only after quorum validation.

This gives:

- protection from single faulty mapper output
- deterministic acceptance criteria
- stronger trust in final aggregated counts

### 8.4 Aggregation Consistency

The Aggregator uses `processedChunks` to ensure at-most-once merge behavior per `chunkId`.

This gives chunk-level idempotency for aggregation.

### 8.5 File Consistency Assumption

The current system assumes all participating nodes can read the same `sourceFilePath` and that the file contents do not change while the job is executing.

Under that assumption:

- mapper and validator computations are consistent
- repeated chunk reconstruction returns the same input lines

### 8.6 Current Guarantee Summary

The system currently provides:

- eventual consistency for cluster membership snapshots
- eventual consistency for leader convergence
- quorum-validated consistency for accepted chunk results
- chunk-level idempotent aggregation

It does not yet provide:

- durable state consistency across restarts
- exactly-once end-to-end job execution
- strong consistency under concurrent file mutation
