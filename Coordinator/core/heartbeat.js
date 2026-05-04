const cluster = require("./clusterManager");
const proxy = require("../sidecar/proxy");

const HEARTBEAT_INTERVAL_MS = 3000;
const LEADER_TIMEOUT_MS = 7000;

function start() {
    setInterval(async () => {
        const self = cluster.getSelf();
        const snapshot = cluster.getClusterView();
        const nodes = Object.values(snapshot.nodes || {});

        if (cluster.isLeader()) {
            for (const node of nodes) {
                if (node.id === self.id) continue;

                try {
                    const res = await proxy.get(`http://localhost:${node.port}/heartbeat`, { timeout: 1000, retries: 0 });
                    cluster.monitorNodeHealth(res);
                } catch {
                    cluster.markNodeDead(node.id, "heartbeat-timeout");
                }
            }

            return;
        }

        const leaderId = snapshot.leaderId;
        if (!leaderId) {
            cluster.handleLeaderFailure();
            return;
        }

        const leaderPort = Number(String(leaderId).split(":")[1]);

        try {
            const res = await proxy.get(`http://localhost:${leaderPort}/heartbeat`, { timeout: 1000, retries: 0 });
            cluster.handleLeaderHeartbeat(res.leaderId);
            cluster.monitorNodeHealth(res);
        } catch {
            cluster.handleLeaderFailure();
        }

        if (Date.now() - cluster.getClusterView().lastLeaderHeartbeat > LEADER_TIMEOUT_MS) {
            cluster.handleLeaderFailure();
        }
    }, HEARTBEAT_INTERVAL_MS);
}

module.exports = { start };
