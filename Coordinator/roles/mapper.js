const proxy = require("../sidecar/proxy");
const summarizeChunk = require("../utils/summarizer");
const readChunk = require("../utils/chunkReader");

module.exports = async (req, res) => {
    const {
        chunk,
        jobId,
        chunkId,
        startLine,
        endLine,
        validators,
        aggregator,
        sourceFilePath,
    } = req.body;
    const materializedChunk = readChunk({ chunk, sourceFilePath, startLine, endLine });

    console.log(`Mapper processing ${chunkId} (${startLine}-${endLine})`);

    const result = summarizeChunk(materializedChunk);

    console.log(`Mapper result for ${chunkId}:`, result);

    console.log(`Sending validation requests for ${chunkId} to validators:`, validators);

    const validatorRequests = validators.map(port =>
        proxy.send(`http://localhost:${port}/validate`, {
            jobId,
            chunkId,
            sourceFilePath,
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
        console.log(`Chunk ${chunkId} accepted by quorum (${accepted}/${validators.length}). Sending to aggregator ${aggregator}.`);
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

    console.log(`Chunk ${chunkId} rejected by quorum (${accepted}/${validators.length}).`);

    res.status(409).json({
        status: "rejected",
        chunkId,
        accepted,
        quorum,
        votes,
    });
};
