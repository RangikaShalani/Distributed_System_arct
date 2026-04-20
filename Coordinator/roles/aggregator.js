const finalResults = {};

module.exports = (req, res) => {
    const { jobId, chunkId, result, validation } = req.body;

    if (!finalResults[jobId]) {
        finalResults[jobId] = {
            processedChunks: new Set(),
            totals: {},
            uniqueMessages: {},
        };
    }

    const job = finalResults[jobId];

    if (job.processedChunks.has(chunkId)) {
        return res.send("Duplicate");
    }

    job.processedChunks.add(chunkId);

    for (const [severity, summary] of Object.entries(result)) {
        if (!job.totals[severity]) {
            job.totals[severity] = 0;
            job.uniqueMessages[severity] = new Set();
        }

        job.totals[severity] += summary.count;
        summary.messages.forEach(message => job.uniqueMessages[severity].add(message));
    }

    const output = Object.entries(job.totals).map(([severity, count]) => ({
        severity,
        count,
        uniqueMessages: job.uniqueMessages[severity].size,
    }));

    console.log(`FINAL RESULT (${jobId})`, output, validation || {});
    res.json({
        status: "aggregated",
        jobId,
        chunkId,
        output,
    });
};
