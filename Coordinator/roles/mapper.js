const parse = require("../utils/parser");
const proxy = require("../sidecar/proxy");

module.exports = async (req, res) => {
    const { chunk, taskId, validators, aggregator } = req.body;

    console.log("Mapper processing chunk...");

    let result = {};

    chunk.forEach(line => {
        const severity = parse(line);

        if (!result[severity]) {
            result[severity] = { count: 0, messages: new Set() };
        }

        result[severity].count++;
        result[severity].messages.add(line);
    });

    Object.keys(result).forEach(k => {
        result[k].messages = Array.from(result[k].messages);
    });

    for (let port of validators) {
        await proxy.send(`http://localhost:${port}/validate`, {
            result,
            taskId,
            aggregator,
        });
    }

    res.send("Mapped");
};