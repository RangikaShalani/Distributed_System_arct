const proxy = require("../sidecar/proxy");
const summarizeChunk = require("../utils/summarizer");
const readChunk = require("../utils/chunkReader");

function uniquePorts(ports = []) {
    return Array.from(new Set(ports.map(port => Number(port)).filter(Number.isFinite)));
}

async function validateWithPorts({
    ports,
    jobId,
    chunkId,
    sourceFilePath,
    mapperResult,
    startLine,
    endLine,
    aggregator,
    expectedValidatorCount,
}) {
    const requests = ports.map(port =>
        proxy.post(`http://localhost:${port}/validate`, {
            jobId,
            chunkId,
            sourceFilePath,
            mapperResult,
            startLine,
            endLine,
            aggregator,
            expectedValidatorCount,
        })
    );

    const settled = await Promise.allSettled(requests);

    return settled.map((item, index) => {
        if (item.status === "fulfilled") {
            return item.value;
        }

        return {
            status: "validator-call-failed",
            validatorPort: ports[index],
            accepted: false,
            error: item.reason?.message || "Validator call failed",
        };
    });
}

module.exports = async (req, res) => {
    const {
        chunk,
        jobId,
        chunkId,
        startLine,
        endLine,
        validators = [],
        allValidatorList = [],
        aggregator,
        sourceFilePath,
    } = req.body;
    const materializedChunk = readChunk({ chunk, sourceFilePath, startLine, endLine });
    const requiredAcceptedCount = Math.max(1, uniquePorts(validators).length);
    const availableValidators = uniquePorts(allValidatorList.length > 0 ? allValidatorList : validators);
    const attemptedValidators = new Set();
    const acceptedValidators = new Set();
    const validatorResponses = [];

    console.log(`Mapper processing ${chunkId} (${startLine}-${endLine})`);

    let candidatePorts = uniquePorts(validators);

    while (acceptedValidators.size < requiredAcceptedCount && candidatePorts.length > 0) {
        const result = summarizeChunk(materializedChunk);
        console.log(`Mapper result for ${chunkId}:`, result);
        console.log(`Sending ${chunkId} to validators:`, candidatePorts);

        for (const port of candidatePorts) {
            attemptedValidators.add(port);
        }

        const responses = await validateWithPorts({
            ports: candidatePorts,
            jobId,
            chunkId,
            sourceFilePath,
            mapperResult: result,
            startLine,
            endLine,
            aggregator,
            expectedValidatorCount: requiredAcceptedCount,
        });

        validatorResponses.push(...responses);

        for (const response of responses) {
            if (response?.accepted && Number.isFinite(Number(response.validatorPort))) {
                acceptedValidators.add(Number(response.validatorPort));
            }
        }

        if (acceptedValidators.size >= requiredAcceptedCount) {
            break;
        }

        const remainingNeeded = requiredAcceptedCount - acceptedValidators.size;
        candidatePorts = availableValidators
            .filter(port => !attemptedValidators.has(port))
            .slice(0, remainingNeeded);

        if (candidatePorts.length > 0) {
            console.log(
                `Chunk ${chunkId} still needs ${remainingNeeded} accepted validator result(s). ` +
                `Retrying with other validators: ${candidatePorts.join(", ")}`
            );
        }
    }

    if (acceptedValidators.size >= requiredAcceptedCount) {
        return res.json({
            status: "validated",
            jobId,
            chunkId,
            acceptedValidatorCount: acceptedValidators.size,
            expectedValidatorCount: requiredAcceptedCount,
            attemptedValidators: Array.from(attemptedValidators),
        });
    }

    return res.status(409).json({
        status: "rejected",
        jobId,
        chunkId,
        acceptedValidatorCount: acceptedValidators.size,
        expectedValidatorCount: requiredAcceptedCount,
        attemptedValidators: Array.from(attemptedValidators),
        validatorResponses,
    });
};
