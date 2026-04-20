const proxy = require("../sidecar/proxy");
const summarizeChunk = require("../utils/summarizer");

module.exports = async (req, res) => {
    const { chunk, jobId, chunkId, startLine, endLine, validators, aggregator } = req.body;

    console.log(`Mapper processing ${chunkId} (${startLine}-${endLine})`);

    const result = summarizeChunk(chunk);
    const validatorRequests = validators.map(port =>
        proxy.send(`http://localhost:${port}/validate`, {
            jobId,
            chunkId,
            chunk,
            mapperResult: result,
            startLine,
            endLine,
        })
    );

    const settled = await Promise.allSettled(validatorRequests);
    const votes = settled
        .filter(item => item.status === "fulfilled")
        .map(item => item.value)
        .filter(Boolean);

    const accepted = votes.filter(vote => vote.accepted).length;
    const quorum = Math.floor(validators.length / 2) + 1;

    if (accepted >= quorum) {
        await proxy.send(`http://localhost:${aggregator}/aggregate`, {
            jobId,
            chunkId,
            result,
            validation: {
                quorum,
                accepted,
                validators,
            },
        });

        return res.json({
            status: "validated",
            chunkId,
            accepted,
            quorum,
        });
    }

    res.status(409).json({
        status: "rejected",
        chunkId,
        accepted,
        quorum,
        votes,
    });
};
