# Distributed Log Processing Design

## 1. Cluster Initialization

### Discovery mechanism
- Every node runs the same codebase and starts with only its listening port.
- On startup, a node probes the configured peer port list (`CLUSTER_PORTS`) through `GET /cluster/status`.
- If it finds a live leader, it joins through `POST /cluster/join`.
- If no leader exists, the node runs a bully-style election where the highest live port becomes the Coordinator.
- The Coordinator assigns runtime roles and broadcasts the full cluster snapshot through `POST /cluster/sync`.

### Dynamic joining
- A new node can start at any time.
- It discovers the current leader by probing peers.
- The leader adds the new node to membership, assigns the role with these priorities:
  - first available non-leader node becomes `AGGREGATOR`
  - next two non-leader nodes become `VALIDATOR`
  - all remaining nodes become `MAPPER`
- After each join, the Coordinator rebroadcasts the new configuration to every participant.

### Failure detection
- The Coordinator heartbeats all known workers through `GET /heartbeat` every 3 seconds.
- Followers heartbeat only the Coordinator.
- Missing heartbeats mark a node as dead.
- If the dead node is the Coordinator, followers trigger leader election.
- If the dead node is the Aggregator, the Coordinator immediately promotes another live node and rebroadcasts the configuration.

## 2. Work Distribution Algorithm

### Chosen method: line-range partitioning
- The Coordinator reads the full file and splits it into contiguous line ranges.
- `chunkSize = ceil(totalLines / mapperCount)`.
- Each mapper receives one chunk plus metadata:
  - `jobId`
  - `chunkId`
  - `startLine`
  - `endLine`
  - validator targets
  - aggregator target

### Why it load-balances well
- Large logs are evenly divided by line count, so each mapper gets approximately the same number of records.
- The algorithm is simple and stable, with very low coordination overhead.
- It works especially well when log parsing cost is close to uniform per line.

### When nodes are added or removed
- New mapper nodes participate in the next job automatically because the Coordinator rebuilds the live mapper list at dispatch time.
- If a mapper dies before a future job, the remaining line ranges are redistributed across the surviving mappers.
- This implementation rebalances between jobs rather than mid-chunk migration.

## 3. Mapper Node Logic

### Parsing algorithm
1. Read each line in the assigned chunk.
2. Detect severity using substring matching:
   - `FATAL -> CRITICAL`
   - `ERROR -> ERROR`
   - `WARNING -> WARNING`
   - `INFO -> INFO`
   - `DEBUG -> DEBUG`
3. Increment the severity counter.
4. Add the raw line to the severity's unique-message set.

### Data structures
- Per mapper chunk summary:
  - object keyed by severity
  - each value stores `count` and `messages`
- During execution, `messages` uses a `Set` to avoid duplicates.
- Before sending over HTTP, the set is converted to a sorted array.

### Concurrency handling inside a mapper
- Each HTTP request is isolated and computes its chunk summary in request-local memory.
- No shared mutable state is used across mapper requests.
- Validator calls are sent concurrently with `Promise.allSettled`, so slow or failed validators do not block other validation responses forever.

### Sequence diagram
```text
Coordinator -> Mapper: /map(chunk, validators, aggregator)
Mapper -> Mapper: summarizeChunk()
Mapper -> Validator A: /validate(chunk, mapperResult)
Mapper -> Validator B: /validate(chunk, mapperResult)
Validator A -> Mapper: accept/reject
Validator B -> Mapper: accept/reject
Mapper -> Mapper: majority/quorum decision
Mapper -> Aggregator: /aggregate(validatedResult)
```

## 4. Validation Phase

### Validation algorithm
- Each mapper sends the original chunk and its computed result to at least two validators.
- Each validator independently recomputes the summary from the raw chunk.
- It compares the recomputed summary with the mapper result.
- It returns `accepted=true` only when both summaries match exactly.

### Majority rule
- Quorum is `floor(numberOfValidators / 2) + 1`.
- With two validators, both must accept.
- With three validators, any two acceptances are enough.

### Mismatch handling
- If quorum is not reached, the mapper marks the chunk as rejected and does not forward it to the Aggregator.
- The rejection payload contains vote details so the Coordinator or operator can inspect failures.

### Consensus
- Consensus is quorum-based at chunk level.
- A chunk is considered valid only after enough validators independently recompute and confirm the mapper output.
- This prevents a single faulty mapper from poisoning the global aggregate.

## 5. Aggregation Phase

### Aggregator behavior
- The Aggregator keeps a per-job structure:
  - `processedChunks` to prevent duplicates
  - `totals` for final severity counts
  - `uniqueMessages` to track unique messages per severity
- Each validated chunk is merged once.
- Final output is logged as severity totals plus unique-message counts.

## 6. Leader Election And Aggregator Failover

### Leader election algorithm
- The system uses a bully-style election.
- Node priority is determined by port number.
- When the Coordinator fails:
  1. followers detect missing leader heartbeat
  2. each follower contacts higher-priority live nodes through `POST /cluster/election`
  3. if no higher-priority node responds, the caller becomes the new Coordinator
  4. the new Coordinator broadcasts itself through `POST /cluster/coordinator` and `POST /cluster/sync`

### Why this works
- Only the highest live node can complete the election without being preempted by a higher one.
- The resulting leader is deterministic and does not require manual user enrollment.

### Aggregator reassignment
- The Coordinator tracks the Aggregator with heartbeats.
- If the Aggregator dies, the Coordinator immediately promotes another live non-leader node.
- The preferred replacement is an existing validator; otherwise any live mapper is promoted.
- The updated cluster view is broadcast to all nodes, so future mapper results target the new Aggregator automatically.

## 7. Communication Model

- Nodes communicate directly over HTTP.
- No manual enrollment step is required after startup.
- Membership, roles, leader identity, and aggregator identity are exchanged through cluster-control endpoints:
  - `GET /cluster/status`
  - `POST /cluster/join`
  - `POST /cluster/sync`
  - `POST /cluster/election`
  - `POST /cluster/coordinator`
  - `GET /heartbeat`
