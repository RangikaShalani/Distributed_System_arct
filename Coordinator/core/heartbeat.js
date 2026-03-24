const axios = require("axios");
const cluster = require("./clusterManager");

function start() {
    setInterval(async () => {
        const nodes = cluster.getNodes();

        for (let port in nodes) {
            try {
                await axios.get(`http://localhost:${port}/heartbeat`);
                nodes[port].status = "alive";
            } catch {
                nodes[port].status = "dead";
                console.log(`Node ${port} is DEAD`);
            }
        }
    }, 5000);
}

module.exports = { start };