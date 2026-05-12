const fs = require("fs");
const readline = require("readline");
const proxy = require("../sidecar/proxy");

const DEFAULT_PORT_RANGE = { start: 8000, end: 8020 };
const HOST = "localhost";
const CLUSTER_SYNC_DELAY_MS = 200;
const COORDINATOR_ANNOUNCE_WAIT_MS = 1800;

const state = {
    selfPort: null,
    selfId: null,
    filePath: null,
    role: "UNASSIGNED",
    leaderId: null,
    aggregatorId: null,
    electionTerm: 0,
    version: 0,
    nodes: {},
    electionInProgress: false,
    lastLeaderHeartbeat: 0,
};
let pendingFilePrompt = null;

function setLeader(leaderId, options = {}) {
    const previousLeaderId = state.leaderId;
    state.leaderId = leaderId || null;

    if (!state.leaderId || previousLeaderId === state.leaderId) {
        return;
    }

    const leaderPort = nodeIdToPort(state.leaderId);
    console.log(`${leaderPort} is the leader`);
}

function init(port) {
    state.selfPort = Number(port);
    state.selfId = toNodeId(state.selfPort);
    state.filePath = null;
    state.lastLeaderHeartbeat = Date.now();

    ensureNodeRecord(state.selfPort, {
        role: state.role,
        status: "alive",
        lastSeen: Date.now(),
    });

    setTimeout(() => {
        bootstrapCluster().catch(err => {
            console.log("Cluster bootstrap failed:", err.message);
            becomeLeader("bootstrap-fallback");
        });
    }, CLUSTER_SYNC_DELAY_MS);
}

async function bootstrapCluster() {
    const discovered = await discoverPeers();
    const snapshots = discovered
        .filter(peer => peer.snapshot && peer.snapshot.nodes)
        .sort((a, b) => b.snapshot.electionTerm - a.snapshot.electionTerm || b.port - a.port);

    const leaderPeer = snapshots.find(peer => peer.snapshot.leaderId && isAliveRole(peer.snapshot.nodes, peer.snapshot.leaderId));

    if (leaderPeer) {
        applySnapshot(leaderPeer.snapshot);
        await joinLeader(nodeIdToPort(leaderPeer.snapshot.leaderId));
        return;
    }

    if (snapshots.length > 0) {
        applySnapshot(snapshots[0].snapshot);
        await startElection("bootstrap-no-leader");
        return;
    }

    becomeLeader("bootstrap-no-peers");
}

async function discoverPeers() {
    const peers = [];

    for (const port of getSeedPorts()) {
        if (port === state.selfPort) continue;

        try {
            const snapshot = await proxy.get(buildUrl(port, "/cluster/status"), { timeout: 800 });
            peers.push({ port, snapshot });
        } catch {
            // Peer not reachable yet.
        }
    }

    return peers;
}

async function joinLeader(leaderPort) {
    const res = await proxy.post(buildUrl(leaderPort, "/cluster/join"), {
        port: state.selfPort,
        nodeId: state.selfId,
    });

    if (res.snapshot) {
        applySnapshot(res.snapshot);
    }

    state.role = res.role || state.role;
    ensureNodeRecord(state.selfPort, {
        role: state.role,
        status: "alive",
        lastSeen: Date.now(),
    });

    console.log(`Joined cluster via leader ${leaderPort} as ${state.role}`);
}

function registerNode(req, res) {
    if (!isLeader()) {
        return res.status(409).json({
            error: "NOT_LEADER",
            leaderId: state.leaderId,
        });
    }

    const { port, nodeId } = req.body;
    const joinPort = Number(port);
    const joinId = nodeId || toNodeId(joinPort);

    state.nodes[joinId] = {
        id: joinId,
        port: joinPort,
        role: "UNASSIGNED",
        status: "alive",
        lastSeen: Date.now(),
    };

    bumpVersion();
    rebalanceClusterRoles();
    broadcastClusterConfig();

    const assignedRole = assignRole(joinId);
    console.log(`Node ${joinPort} -> ${assignedRole} joined the cluster`);

    res.json({
        role: assignedRole,
        snapshot: buildSnapshot(),
    });
}

function clusterStatus(req, res) {
    res.json(buildSnapshot());
}

function syncCluster(req, res) {
    applySnapshot(req.body);
    res.json({
        ok: true,
        nodeId: state.selfId,
        role: state.role,
    });
}

function electionHandler(req, res) {
    const { candidateId, term } = req.body;
    const candidatePort = nodeIdToPort(candidateId);
    const shouldReplyOk = state.selfPort > candidatePort;

    ensureNodeRecord(candidatePort, {
        role: state.nodes[candidateId]?.role || "UNASSIGNED",
        status: "alive",
        lastSeen: Date.now(),
    });

    if (term > state.electionTerm) {
        state.electionTerm = term;
    }

    if (!shouldReplyOk) {
        return res.status(204).end();
    }

    res.json({
        ok: true,
        responderId: state.selfId,
        willRunElection: true,
    });

    if (!state.electionInProgress) {
        setTimeout(() => {
            startElection("received-election").catch(err => {
                console.log("Election retry failed:", err.message);
            });
        }, 0);
    }
}

function coordinatorHandler(req, res) {
    const snapshot = req.body;
    applySnapshot(snapshot);
    state.electionInProgress = false;
    res.json({
        ok: true,
        leaderId: state.leaderId,
    });
}

async function startElection(reason = "leader-failed") {
    if (state.electionInProgress) return;

    state.electionInProgress = true;
    state.electionTerm += 1;
    state.leaderId = null;
    console.log(`Election started (${reason}) term=${state.electionTerm}`);

    const peers = getAliveNodes()
        .filter(node => node.id !== state.selfId);

    let receivedAck = false;

    for (const node of peers) {
        try {
            console.log(`Sending election message to ${node.port} (term ${state.electionTerm})`);
            const res = await proxy.post(buildUrl(node.port, "/cluster/election"), {
                candidateId: state.selfId,
                term: state.electionTerm,
            }, { timeout: 1000, retries: 0 });

            if (res?.ok && res?.willRunElection) {
                console.log(`Received OK from higher node ${node.port}`);
                receivedAck = true;
            }
        } catch {
            markNodeDead(node.id, "election-timeout");
        }
    }

    if (!receivedAck) {
        becomeLeader(reason);
        state.electionInProgress = false;
        return;
    }

    setTimeout(() => {
        if (!isLeader() && leaderIsMissing()) {
            state.electionInProgress = false;
            startElection("coordinator-timeout").catch(err => {
                console.log("Coordinator timeout election failed:", err.message);
            });
            return;
        }

        state.electionInProgress = false;
    }, COORDINATOR_ANNOUNCE_WAIT_MS);
}

function becomeLeader(reason = "promotion") {
    setLeader(state.selfId, { source: reason });
    state.role = "COORDINATOR";
    state.electionTerm += 1;
    state.electionInProgress = false;
    state.lastLeaderHeartbeat = Date.now();

    ensureNodeRecord(state.selfPort, {
        role: "COORDINATOR",
        status: "alive",
        lastSeen: Date.now(),
    });

    rebalanceClusterRoles();
    bumpVersion();

    console.log(`I am the new leader (${reason})`);
    announceCoordinator().catch(err => {
        console.log("Coordinator announcement failed:", err.message);
    });
    broadcastClusterConfig().catch(err => {
        console.log("Cluster config broadcast failed:", err.message);
    });
}

async function announceCoordinator() {
    const snapshot = buildSnapshot();
    const peers = getAliveNodes().filter(node => node.id !== state.selfId);

    await Promise.allSettled(
        peers.map(node =>
            proxy.post(buildUrl(node.port, "/cluster/coordinator"), snapshot, { timeout: 1000, retries: 0 })
                .catch(() => {
                    markNodeDead(node.id, "announce-failed");
                })
        )
    );
}

async function broadcastClusterConfig() {
    const snapshot = buildSnapshot();
    const peers = getAliveNodes().filter(node => node.id !== state.selfId);

    await Promise.allSettled(
        peers.map(node =>
            proxy.post(buildUrl(node.port, "/cluster/sync"), snapshot, { timeout: 1000, retries: 0 })
                .catch(() => {
                    markNodeDead(node.id, "sync-failed");
                })
        )
    );
}

function heartbeatAck(req, res) {
    ensureNodeRecord(state.selfPort, {
        role: state.role,
        status: "alive",
        lastSeen: Date.now(),
    });

    res.json({
        ok: true,
        nodeId: state.selfId,
        role: state.role,
        leaderId: state.leaderId,
        aggregatorId: state.aggregatorId,
        term: state.electionTerm,
        version: state.version,
    });
}

function handleLeaderHeartbeat(leaderId) {
    state.lastLeaderHeartbeat = Date.now();
    if (leaderId) {
        setLeader(leaderId, { source: "heartbeat" });
    }
}

function handleLeaderFailure() {
    if (isLeader()) return;
    startElection("heartbeat-leader-dead").catch(err => {
        console.log("Leader failover failed:", err.message);
    });
}

function monitorNodeHealth(report = {}) {
    const { nodeId, role, leaderId } = report;
    if (!nodeId) return;

    ensureNodeRecord(nodeIdToPort(nodeId), {
        role: role || state.nodes[nodeId]?.role || "UNASSIGNED",
        status: "alive",
        lastSeen: Date.now(),
    });

    if (leaderId) {
        setLeader(leaderId, { source: "health-report" });
    }
}

function markNodeAlive(nodeId, details = {}) {
    const current = state.nodes[nodeId] || {
        id: nodeId,
        port: nodeIdToPort(nodeId),
        role: "UNASSIGNED",
    };

    state.nodes[nodeId] = {
        ...current,
        ...details,
        status: "alive",
        lastSeen: Date.now(),
    };
}

function markNodeDead(nodeId, reason = "heartbeat-missed") {
    if (!state.nodes[nodeId]) return;
    if (state.nodes[nodeId].status === "dead") return;

    state.nodes[nodeId].status = "dead";
    console.log(`Node ${state.nodes[nodeId].port} is DEAD (${reason})`);

    if (nodeId === state.aggregatorId && isLeader()) {
        reassignAggregator();
    }

    if (nodeId === state.leaderId && !isLeader()) {
        handleLeaderFailure();
    }

    bumpVersion();
    if (isLeader()) {
        rebalanceClusterRoles();
        broadcastClusterConfig();
    }
}

function ensureNodeRecord(portOrId, details = {}) {
    const nodeId = String(portOrId).includes(":") ? String(portOrId) : toNodeId(Number(portOrId));
    markNodeAlive(nodeId, details);

    if (nodeId === state.selfId) {
        state.role = state.nodes[nodeId].role || state.role;
    }
}

function assignRole(nodeId) {
    const candidates = getAliveNodes()
        .filter(node => node.id !== state.leaderId)
        .map(node => ({ ...node }));

    if (!candidates.find(node => node.id === nodeId)) {
        candidates.push({
            id: nodeId,
            port: nodeIdToPort(nodeId),
            role: "UNASSIGNED",
            status: "alive",
        });
    }

    const aggregatorId =
        state.aggregatorId && candidates.find(node => node.id === state.aggregatorId)
            ? state.aggregatorId
            : chooseAggregatorCandidate(candidates)?.id || null;

    const rolePlan = buildRolePlan(candidates, aggregatorId);
    return rolePlan[nodeId] || "UNASSIGNED";
}

function rebalanceClusterRoles() {
    const aliveNodes = getAliveNodes().sort((a, b) => a.port - b.port);
    const candidates = aliveNodes.filter(node => node.id !== state.leaderId);

    if (state.nodes[state.leaderId]) {
        state.nodes[state.leaderId].role = "COORDINATOR";
    }

    let aggregator = candidates.find(node => node.id === state.aggregatorId && node.status !== "dead");
    if (!aggregator && candidates.length > 0) {
        aggregator = chooseAggregatorCandidate(candidates);
    }

    state.aggregatorId = aggregator ? aggregator.id : null;

    const rolePlan = buildRolePlan(candidates, state.aggregatorId);
    for (const node of candidates) {
        state.nodes[node.id].role = rolePlan[node.id] || "UNASSIGNED";
    }

    if (state.nodes[state.selfId]) {
        state.role = state.nodes[state.selfId].role;
    }
}

function buildRolePlan(candidates, aggregatorId) {
    const sortedCandidates = [...candidates].sort((a, b) => a.port - b.port);
    const rolePlan = {};

    if (aggregatorId && sortedCandidates.find(node => node.id === aggregatorId)) {
        rolePlan[aggregatorId] = "AGGREGATOR";
    }

    const aliveRoles = aggregatorId ? ["AGGREGATOR"] : [];
    const workers = sortedCandidates.filter(node => node.id !== aggregatorId);

    for (const node of workers) {
        const nextRole = chooseWorkerRole(aliveRoles);
        rolePlan[node.id] = nextRole;
        aliveRoles.push(nextRole);
    }

    return rolePlan;
}

function chooseWorkerRole(aliveRoles) {
    const validatorCount = aliveRoles.filter(role => role === "VALIDATOR").length;
    const mapperCount = aliveRoles.filter(role => role === "MAPPER").length;

    if (validatorCount < 2) return "VALIDATOR";
    if (mapperCount < 1) return "MAPPER";
    if (mapperCount <= validatorCount) return "MAPPER";
    return "VALIDATOR";
}

function chooseAggregatorCandidate(nodes) {
    if (nodes.length === 0) return null;
    return [...nodes].sort((a, b) => a.port - b.port)[0];
}

function reassignAggregator() {
    const candidates = getAliveNodes().filter(node => node.id !== state.leaderId);
    if (candidates.length === 0) {
        state.aggregatorId = null;
        return null;
    }

    const nextAggregator = chooseAggregatorCandidate(candidates);
    state.aggregatorId = nextAggregator.id;
    state.nodes[nextAggregator.id].role = "AGGREGATOR";
    console.log(`Aggregator reassigned to ${nextAggregator.port}`);
    return nextAggregator.id;
}

async function startJob(req, res) {
    if (!isLeader()) {
        console.log("Received job start request but not leader. Rejecting.");
        return res.status(403).send("Not coordinator");
    }

    rebalanceClusterRoles();

    const mappers = getNodesByRole("MAPPER");
    const validators = getNodesByRole("VALIDATOR");
    const aggregator = getAggregator();

    if (!aggregator) {
        console.log("Received job start request but no aggregator node available. Rejecting.");
        console.log("Available nodes:", getAliveNodes().map(node => `${node.port}(${node.role})`));
        return res.status(400).send("No aggregator node available. Please ensure at least minimal cluster setup is available.");
    }

    if (validators.length < 2) {
        console.log("Received job start request but not enough validator nodes available. Rejecting.");
        console.log("Available nodes:", getAliveNodes().map(node => `${node.port}(${node.role})`));
        return res.status(400).send("At least two validator nodes and one mapper are required. Please ensure at least minimal cluster setup is available.");
    }

    if (mappers.length === 0) {
        console.log("Received job start request but no mapper nodes available. Rejecting.");
        console.log("Available nodes:", getAliveNodes().map(node => `${node.port}(${node.role})`));
        return res.status(400).send("No mapper nodes available. Please ensure at least minimal cluster setup is available.");
    }

    try {
        state.filePath = await promptForJobFilePath();
    } catch (err) {
        console.log("Failed to read file path from terminal:", err.message);
        return res.status(500).send("Failed to read file path from terminal");
    }


    const file = fs.readFileSync(state.filePath, "utf-8");
    const lines = file.split(/\r?\n/).filter(Boolean);
    const chunkSize = Math.ceil(lines.length / mappers.length);
    const jobId = `job-${Date.now()}`;

    console.log(`Starting ${jobId} with ${mappers.length} mapper(s), ${validators.length} validator(s), aggregator ${aggregator.port}`);

    let index = 0;
    const dispatches = [];

    mappers.forEach((mapper, mapperIndex) => {

        const chunk = lines.slice(index, index + chunkSize);
        const startLine = index + 1;
        const endLine = index + chunk.length;
        index += chunkSize;

        const assignedValidators = chooseValidatorsForMapper(validators, mapperIndex);

        console.log(`Dispatching ${jobId}-chunk-${mapperIndex + 1} to mapper ${mapper.port} with validators:`, assignedValidators.map(node => node.port));

        dispatches.push(proxy.post(buildUrl(mapper.port, "/map"), {
            jobId,
            chunkId: `${jobId}-chunk-${mapperIndex + 1}`,
            startLine,
            endLine,
            sourceFilePath: state.filePath,
            validators: assignedValidators.map(node => node.port),
            allValidatorList: validators.map(node => node.port),
            aggregator: aggregator.port,
        }));
    });

    await Promise.allSettled(dispatches);
    res.send(`Job ${jobId} started`);
}

async function promptForJobFilePath() {
    if (pendingFilePrompt) {
        return pendingFilePrompt;
    }

    pendingFilePrompt = (async () => {
        while (true) {
            const candidatePath = await askTerminalQuestion("Enter log file path for this job: ");
            const trimmedPath = candidatePath.trim();

            if (!trimmedPath) {
                console.log("File path cannot be empty. Please try again.");
                continue;
            }

            if (!fs.existsSync(trimmedPath)) {
                console.log(`File not found: ${trimmedPath}`);
                continue;
            }

            const stats = fs.statSync(trimmedPath);
            if (!stats.isFile()) {
                console.log(`Not a file: ${trimmedPath}`);
                continue;
            }

            console.log(`Job file selected: ${trimmedPath}`);
            return trimmedPath;
        }
    })();

    try {
        return await pendingFilePrompt;
    } finally {
        pendingFilePrompt = null;
    }
}

function askTerminalQuestion(question) {
    const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout,
    });

    return new Promise(resolve => {
        rl.question(question, answer => {
            rl.close();
            resolve(answer);
        });
    });
}

function chooseValidatorsForMapper(validators, mapperIndex) {
    const required = Math.min(2, validators.length);
    const selected = [];

    for (let i = 0; i < required; i += 1) {
        selected.push(validators[(mapperIndex + i) % validators.length]);
    }

    return selected;
}

function buildSnapshot() {
    return {
        selfId: state.selfId,
        leaderId: state.leaderId,
        aggregatorId: state.aggregatorId,
        electionTerm: state.electionTerm,
        version: state.version,
        nodes: state.nodes,
    };
}

function applySnapshot(snapshot = {}) {
    if (!snapshot.nodes) return;

    const isNewer =
        snapshot.electionTerm > state.electionTerm ||
        (snapshot.electionTerm === state.electionTerm && (snapshot.version || 0) >= state.version);

    if (!isNewer) return;

    state.nodes = snapshot.nodes;
    setLeader(snapshot.leaderId || state.leaderId, { source: "snapshot-sync" });
    state.aggregatorId = snapshot.aggregatorId || null;
    state.electionTerm = snapshot.electionTerm || state.electionTerm;
    state.version = snapshot.version || state.version;

    if (!state.nodes[state.selfId]) {
        ensureNodeRecord(state.selfPort, {
            role: state.role,
            status: "alive",
            lastSeen: Date.now(),
        });
    }

    state.role = state.nodes[state.selfId]?.role || state.role;
}

function buildUrl(port, path) {
    return `http://${HOST}:${port}${path}`;
}

function getSeedPorts() {
    if (process.env.CLUSTER_PORTS) {
        return process.env.CLUSTER_PORTS
            .split(",")
            .map(value => Number(value.trim()))
            .filter(Number.isFinite);
    }

    const ports = [];
    for (let port = DEFAULT_PORT_RANGE.start; port <= DEFAULT_PORT_RANGE.end; port += 1) {
        ports.push(port);
    }
    return ports;
}

function toNodeId(port) {
    return `${HOST}:${port}`;
}

function nodeIdToPort(nodeId) {
    return Number(String(nodeId).split(":")[1]);
}

function getAliveNodes() {
    return Object.values(state.nodes).filter(node => node.status !== "dead");
}

function getNodesByRole(role) {
    return getAliveNodes().filter(node => node.role === role);
}

function getAggregator() {
    return state.aggregatorId ? state.nodes[state.aggregatorId] : null;
}

function leaderIsMissing() {
    return !state.leaderId || !isAliveRole(state.nodes, state.leaderId);
}

function isAliveRole(nodes, nodeId) {
    return Boolean(nodes[nodeId] && nodes[nodeId].status !== "dead");
}

function isLeader() {
    return state.selfId === state.leaderId && state.role === "COORDINATOR";
}

function bumpVersion() {
    state.version += 1;
}

function getSelf() {
    return {
        id: state.selfId,
        port: state.selfPort,
        role: state.role,
    };
}

function getClusterView() {
    return {
        ...buildSnapshot(),
        lastLeaderHeartbeat: state.lastLeaderHeartbeat,
    };
}

module.exports = {
    init,
    registerNode,
    clusterStatus,
    syncCluster,
    electionHandler,
    coordinatorHandler,
    heartbeatAck,
    startJob,
    handleLeaderHeartbeat,
    handleLeaderFailure,
    monitorNodeHealth,
    markNodeDead,
    markNodeAlive,
    getNodesByRole,
    getAggregator,
    getSelf,
    getClusterView,
    isLeader,
};
