const summarizeChunk = require("../utils/summarizer");

module.exports = async (req, res) => {
    const { jobId, chunkId, chunk, mapperResult, startLine, endLine } = req.body;

    console.log(`Validator checking ${chunkId} (${startLine}-${endLine})`);

    const recomputed = summarizeChunk(chunk);
    const accepted = JSON.stringify(recomputed) === JSON.stringify(mapperResult);

    res.json({
        jobId,
        chunkId,
        accepted,
        validatorAt: Date.now(),
        reason: accepted ? "MATCH" : "MISMATCH",
    });
};
