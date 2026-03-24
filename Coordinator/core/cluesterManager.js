const fs = require("fs");
const proxy = require("../sidecar/proxy");

let PORT;
let FILE_PATH;
let role = "UNASSIGNED";
let nodes = {};

function init(port, filePath) {
    PORT = port;
    FILE_PATH = filePath;

    if (PORT == 8000) {
        role = "COORDINATOR";
        console.log("I am Coordinator");

        if (FILE_PATH) {
            console.log("File:", FILE_PATH);
        }
    } else {
        register();
    }
}

async function register() {
    try {
        const res = await proxy.send("http://localhost:8000/register", {
            port: PORT,
        });

        role = res.role;
        console.log(`Assigned role: ${role}`);
    } catch {
        console.log("Coordinator not available");
    }
}

function registerNode(req, res) {
    const { port } = req.body;

    const assignedRole = assignRole();

    nodes[port] = {
        role: assignedRole,
        status: "alive",
    };

    console.log(`Node ${port} → ${assignedRole}`);

    res.json({ role: assignedRole });
}

function assignRole() {
    const roles = Object.values(nodes).map(n => n.role);

    if (!roles.includes("AGGREGATOR")) return "AGGREGATOR";
    if (roles.filter(r => r === "VALIDATOR").length < 2) return "VALIDATOR";
    return "MAPPER";
}

// ---------------- START JOB ----------------
async function startJob(req, res) {
    if (role !== "COORDINATOR") return res.send("Not coordinator");

    if (!FILE_PATH) return res.send("No file provided");

    const file = fs.readFileSync(FILE_PATH, "utf-8");
    const lines = file.split("\n");

    const mappers = Object.entries(nodes)
        .filter(([_, n]) => n.role === "MAPPER");

    const validators = Object.entries(nodes)
        .filter(([_, n]) => n.role === "VALIDATOR")
        .map(([port]) => port);

    const aggregator = Object.entries(nodes)
        .find(([_, n]) => n.role === "AGGREGATOR")[0];

    console.log("Mappers:", mappers.length);
    console.log("Validators:", validators);
    console.log("Aggregator:", aggregator);

    const chunkSize = Math.ceil(lines.length / mappers.length);

    let index = 0;
    const taskId = Date.now();

    for (let [port] of mappers) {
        const chunk = lines.slice(index, index + chunkSize);
        index += chunkSize;

        await proxy.send(`http://localhost:${port}/map`, {
            chunk,
            taskId,
            validators,
            aggregator,
        });
    }

    res.send("Job Started");
}

module.exports = {
    init,
    registerNode,
    startJob,
    getNodes: () => nodes,
};