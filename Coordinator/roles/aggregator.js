const finalResults = {};

function getJobState(jobId) {
    if (!finalResults[jobId]) {
        finalResults[jobId] = {
            processedChunks: new Set(),
            totals: {},
            uniqueMessages: {},
            validationReports: {},
        };
    }

    return finalResults[jobId];
}

function buildOutput(job) {
    return Object.entries(job.totals).map(([severity, count]) => ({
        severity,
        count,
        uniqueMessages: job.uniqueMessages[severity].size,
    }));
}

function mergeResult(job, result = {}) {
    for (const [severity, summary] of Object.entries(result)) {
        if (!job.totals[severity]) {
            job.totals[severity] = 0;
            job.uniqueMessages[severity] = new Set();
        }

        job.totals[severity] += summary.count;
        (summary.messages || []).forEach(message => job.uniqueMessages[severity].add(message));
    }
}

module.exports = (req, res) => {
    const {
        jobId,
        chunkId,
        validatorPort,
        mapperResult,
        recomputedResult,
        expectedValidatorCount = 2,
        reason,
    } = req.body;

    // if (kind !== "validation-report") {
    //     return res.status(400).json({
    //         error: "UNSUPPORTED_AGGREGATION_KIND",
    //         kind,
    //     });
    // }

    const job = getJobState(jobId);

    if (job.processedChunks.has(chunkId)) {
        console.log(`Aggregator ignoring duplicate finalized chunk ${chunkId}`);
        return res.json({
            status: "duplicate-finalized-chunk",
            jobId,
            chunkId,
        });
    }

    if (!job.validationReports[chunkId]) {
        job.validationReports[chunkId] = {
            expectedValidatorCount,
            mapperResult,
            reportsByValidator: {},
            finalized: false,
        };
    }

    const chunkValidation = job.validationReports[chunkId];
    const validatorKey = String(validatorPort);

    if (chunkValidation.finalized) {
        console.log(`Aggregator ignoring late validator report for finalized chunk ${chunkId}`);
        return res.json({
            status: "late-report-ignored",
            jobId,
            chunkId,
        });
    }

    if (chunkValidation.reportsByValidator[validatorKey]) {
        console.log(`Aggregator ignoring duplicate validator ${validatorPort} report for ${chunkId}`);
        return res.json({
            status: "duplicate-validator-report",
            jobId,
            chunkId,
            validatorPort,
        });
    }

    chunkValidation.reportsByValidator[validatorKey] = {
        validatorPort,
        recomputedResult,
        reason: reason || "MATCH",
        receivedAt: Date.now(),
    };

    const reports = Object.values(chunkValidation.reportsByValidator);
    console.log(
        `Aggregator received accepted validator report for ${chunkId} from ${validatorPort}. ` +
        `reports=${reports.length}/${chunkValidation.expectedValidatorCount}`
    );

    if (reports.length < chunkValidation.expectedValidatorCount) {
        return res.status(202).json({
            status: "waiting-for-validator-reports",
            jobId,
            chunkId,
            receivedReports: reports.length,
            expectedValidatorCount: chunkValidation.expectedValidatorCount,
        });
    }

    chunkValidation.finalized = true;
    mergeResult(job, chunkValidation.mapperResult);
    job.processedChunks.add(chunkId);

    const output = buildOutput(job);
    console.log(`FINAL RESULT (${jobId})`, output);

    return res.json({
        status: "aggregated",
        jobId,
        chunkId,
        acceptedCount: reports.length,
        expectedValidatorCount: chunkValidation.expectedValidatorCount,
        output,
    });
};
