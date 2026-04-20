const fs = require("fs");
const axios = require("axios");
const proxy = require("../sidecar/proxy");

const DEFAULT_PORT_RANGE = { start: 8000, end: 8010 };
const HOST = "localhost";
const CLUSTER_SYNC_DELAY_MS = 200;

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

function init(port, filePath) {
    state.selfPort = Number(port);
    state.selfId = toNodeId(state.selfPort);
    state.filePath = filePath || null;
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

    const higherPeers = snapshots.filter(peer => peer.port > state.selfPort);
    if (higherPeers.length > 0) {
        applySnapshot(higherPeers[0].snapshot);
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
            const res = await axios.get(buildUrl(port, "/cluster/status"), { timeout: 800 });
            peers.push({ port, snapshot: res.data });
        } catch {
            // Peer not reachable yet.
        }
    }

    return peers;
}

async function joinLeader(leaderPort) {
    const res = await proxy.send(buildUrl(leaderPort, "/cluster/join"), {
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
    const assignedRole = assignRole(joinId);

    state.nodes[joinId] = {
        id: joinId,
        port: joinPort,
        role: assignedRole,
        status: "alive",
        lastSeen: Date.now(),
    };

    bumpVersion();
    console.log(`Node ${joinPort} -> ${assignedRole}`);
    rebalanceClusterRoles();
    broadcastClusterConfig();

    res.json({
        role: state.nodes[joinId].role,
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
    const shouldTakeOver = state.selfPort > candidatePort;

    ensureNodeRecord(candidatePort, {
        role: state.nodes[candidateId]?.role || "UNASSIGNED",
        status: "alive",
        lastSeen: Date.now(),
    });

    if (term > state.electionTerm) {
        state.electionTerm = term;
    }

    res.json({
        ok: true,
        responderId: state.selfId,
        willRunElection: shouldTakeOver,
    });

    if (shouldTakeOver && !state.electionInProgress) {
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
    res.json({
        ok: true,
        leaderId: state.leaderId,
    });
}

async function startElection(reason = "leader-failed") {
    if (state.electionInProgress) return;

    state.electionInProgress = true;
    state.electionTerm += 1;
    console.log(`Election started (${reason}) term=${state.electionTerm}`);

    const higherNodes = getAliveNodes()
        .filter(node => node.port > state.selfPort);

    let receivedAck = false;

    for (const node of higherNodes) {
        try {
            const res = await axios.post(buildUrl(node.port, "/cluster/election"), {
                candidateId: state.selfId,
                term: state.electionTerm,
            }, { timeout: 1000 });

            if (res.data && res.data.ok) {
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
    }, 1800);
}

function becomeLeader(reason = "promotion") {
    state.leaderId = state.selfId;
    state.role = "COORDINATOR";
    state.electionTerm += 1;
    state.lastLeaderHeartbeat = Date.now();

    ensureNodeRecord(state.selfPort, {
        role: "COORDINATOR",
        status: "alive",
        lastSeen: Date.now(),
    });

    rebalanceClusterRoles();
    bumpVersion();

    console.log(`Node ${state.selfPort} became leader (${reason})`);
    announceCoordinator();
    broadcastClusterConfig();
}

function announceCoordinator() {
    const snapshot = buildSnapshot();

    for (const node of getAliveNodes()) {
        if (node.id === state.selfId) continue;

        axios.post(buildUrl(node.port, "/cluster/coordinator"), snapshot, { timeout: 1000 })
            .catch(() => {
                markNodeDead(node.id, "announce-failed");
            });
    }
}

function broadcastClusterConfig() {
    const snapshot = buildSnapshot();

    for (const node of getAliveNodes()) {
        if (node.id === state.selfId) continue;

        axios.post(buildUrl(node.port, "/cluster/sync"), snapshot, { timeout: 1000 })
            .catch(() => {
                markNodeDead(node.id, "sync-failed");
            });
    }
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
        state.leaderId = leaderId;
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
        state.leaderId = leaderId;
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
    const aliveRoles = getAliveNodes()
        .filter(node => node.id !== nodeId)
        .map(node => node.role);

    if (!aliveRoles.includes("AGGREGATOR")) return "AGGREGATOR";
    if (aliveRoles.filter(role => role === "VALIDATOR").length < 2) return "VALIDATOR";
    return "MAPPER";
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

    for (const node of candidates) {
        state.nodes[node.id].role = "MAPPER";
    }

    if (state.aggregatorId) {
        state.nodes[state.aggregatorId].role = "AGGREGATOR";
    }

    const validatorCandidates = candidates
        .filter(node => node.id !== state.aggregatorId)
        .sort((a, b) => a.port - b.port)
        .slice(0, 2);

    for (const node of validatorCandidates) {
        state.nodes[node.id].role = "VALIDATOR";
    }

    if (state.nodes[state.selfId]) {
        state.role = state.nodes[state.selfId].role;
    }
}

function chooseAggregatorCandidate(nodes) {
    const preferred = nodes.find(node => node.role === "VALIDATOR") || nodes[0];
    return preferred;
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
    if (!isLeader()) return res.status(403).send("Not coordinator");
    if (!state.filePath) return res.status(400).send("No file provided");

    rebalanceClusterRoles();

    const mappers = getNodesByRole("MAPPER");
    const validators = getNodesByRole("VALIDATOR");
    const aggregator = getAggregator();

    if (mappers.length === 0) {
        return res.status(400).send("No mapper nodes available");
    }

    if (validators.length < 2) {
        return res.status(400).send("At least two validator nodes are required");
    }

    if (!aggregator) {
        return res.status(400).send("No aggregator node available");
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

        dispatches.push(proxy.send(buildUrl(mapper.port, "/map"), {
            jobId,
            chunkId: `${jobId}-chunk-${mapperIndex + 1}`,
            startLine,
            endLine,
            chunk,
            validators: assignedValidators.map(node => node.port),
            aggregator: aggregator.port,
        }));
    });

    await Promise.allSettled(dispatches);
    res.send(`Job ${jobId} started`);
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
    state.leaderId = snapshot.leaderId || state.leaderId;
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
