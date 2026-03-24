const express = require("express");
const cluster = require("./core/clusterManager");
const heartbeat = require("./core/heartbeat");

const app = express();
app.use(express.json());

const PORT = process.argv[2] || 8000;
const FILE_PATH = process.argv[3]; // only for coordinator

cluster.init(PORT, FILE_PATH);

// heartbeat
heartbeat.start();

// routes
app.post("/register", cluster.registerNode);
app.get("/heartbeat", (req, res) => res.send("alive"));
app.get("/start", cluster.startJob);

app.post("/map", require("./roles/mapper"));
app.post("/validate", require("./roles/validator"));
app.post("/aggregate", require("./roles/aggregator"));

app.listen(PORT, () => {
    console.log(`Node running on ${PORT}`);
});