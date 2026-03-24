const proxy = require("../sidecar/proxy");

module.exports = async (req, res) => {
    const { result, taskId, aggregator } = req.body;

    console.log("Validator checking...");

    const isValid = true; // simple for now

    if (isValid) {
        await proxy.send(`http://localhost:${aggregator}/aggregate`, {
            result,
            taskId,
        });
    }

    res.send("Validated");
};