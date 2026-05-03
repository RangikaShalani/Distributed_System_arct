const summarizeChunk = require("../utils/summarizer");
const readChunk = require("../utils/chunkReader");

module.exports = async (req, res) => {
    const { jobId, chunkId, chunk, mapperResult, startLine, endLine, sourceFilePath } = req.body;
    const materializedChunk = readChunk({ chunk, sourceFilePath, startLine, endLine });

    console.log(`Validator checking ${chunkId} (${startLine}-${endLine})`);
    console.log(`Mapper result:`, mapperResult);
    console.log(`jobId:`, jobId);

    const recomputed = summarizeChunk(materializedChunk);
    console.log(`Recomputed result:`, recomputed);

    const accepted = JSON.stringify(recomputed) === JSON.stringify(mapperResult);
    console.log(`Validation ${accepted ? "ACCEPTED" : "REJECTED"} for ${chunkId}`);

    res.json({
        jobId,
        chunkId,
        accepted,
        validatorAt: Date.now(),
        reason: accepted ? "MATCH" : "MISMATCH",
    });
};
