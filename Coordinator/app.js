const express = require("express");
const cluster = require("./core/clusterManager");
const heartbeat = require("./core/heartbeat");
const { initNodeLogger, requestResponseLogger } = require("./utils/logger");

const app = express();
app.use(express.json({ limit: process.env.JSON_BODY_LIMIT || "50mb" }));

const PORT = process.argv[2] || 8000;

initNodeLogger(PORT);
app.use(requestResponseLogger);

cluster.init(PORT);

// heartbeat
heartbeat.start();

// routes
app.post("/register", cluster.registerNode);
app.post("/cluster/join", cluster.registerNode);
app.get("/cluster/status", cluster.clusterStatus);
app.post("/cluster/sync", cluster.syncCluster);
app.post("/cluster/election", cluster.electionHandler);
app.post("/cluster/coordinator", cluster.coordinatorHandler);
app.get("/heartbeat", cluster.heartbeatAck);
app.get("/start", cluster.startJob);

app.post("/map", require("./roles/mapper"));
app.post("/validate", require("./roles/validator"));
app.post("/aggregate", require("./roles/aggregator"));

app.listen(PORT, () => {
    console.log(`Node running on ${PORT}`);
});
