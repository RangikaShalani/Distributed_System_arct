const proxy = require("../sidecar/proxy");
const summarizeChunk = require("../utils/summarizer");
const readChunk = require("../utils/chunkReader");

function getSelfPort() {
    return Number(process.argv[2] || 0);
}

module.exports = async (req, res) => {
    const {
        jobId,
        chunkId,
        chunk,
        mapperResult,
        startLine,
        endLine,
        sourceFilePath,
        aggregator,
        expectedValidatorCount = 2,
    } = req.body;
    const materializedChunk = readChunk({ chunk, sourceFilePath, startLine, endLine });
    const recomputed = summarizeChunk(materializedChunk);
    const accepted = JSON.stringify(recomputed) === JSON.stringify(mapperResult);
    const validatorPort = getSelfPort();

    console.log(`Validator checking ${chunkId} (${startLine}-${endLine})`);
    console.log(`Mapper result:`, mapperResult);
    console.log(`Recomputed result:`, recomputed);
    console.log(`Validation ${accepted ? "ACCEPTED" : "REJECTED"} for ${chunkId} by validator ${validatorPort}`);

    if (!accepted) {
        return res.status(409).json({
            status: "rejected",
            jobId,
            chunkId,
            validatorPort,
            accepted: false,
            reason: "MISMATCH",
        });
    }

    try {
        await proxy.post(`http://localhost:${aggregator}/aggregate`, {
            kind: "validation-report",
            jobId,
            chunkId,
            validatorPort,
            accepted: true,
            mapperResult,
            recomputedResult: recomputed,
            expectedValidatorCount,
            validatorAt: Date.now(),
            reason: "MATCH",
        });
    } catch (err) {
        console.log(`Failed to forward validator report for ${chunkId} to aggregator ${aggregator}:`, err.message);
        return res.status(502).json({
            status: "forward-failed",
            jobId,
            chunkId,
            validatorPort,
            accepted: false,
        });
    }

    return res.status(202).json({
        status: "accepted-forwarded",
        jobId,
        chunkId,
        validatorPort,
        accepted: true,
    });
};
