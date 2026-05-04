const axios = require("axios");
const {
    createRequestId,
    getRequestIdFromHeaders,
    logToFileOnly,
    serializeForLog,
} = require("../utils/logger");

const DEFAULT_TIMEOUT_MS = 3000;
const DEFAULT_RETRIES = 1;
const CIRCUIT_FAILURE_THRESHOLD = 3;
const CIRCUIT_OPEN_MS = 5000;
const metrics = {
    totals: {
        requests: 0,
        successes: 0,
        failures: 0,
        retries: 0,
        circuitOpenRejects: 0,
    },
    byTarget: {},
};
const circuitState = new Map();

function getTargetKey(method, url) {
    return `${method.toUpperCase()} ${url}`;
}

function getMetricBucket(targetKey) {
    if (!metrics.byTarget[targetKey]) {
        metrics.byTarget[targetKey] = {
            requests: 0,
            successes: 0,
            failures: 0,
            retries: 0,
            circuitOpenRejects: 0,
            totalLatencyMs: 0,
            lastError: null,
            lastRequestAt: null,
            lastSuccessAt: null,
        };
    }

    return metrics.byTarget[targetKey];
}

function getCircuitEntry(targetKey) {
    if (!circuitState.has(targetKey)) {
        circuitState.set(targetKey, {
            failures: 0,
            state: "CLOSED",
            openedAt: 0,
        });
    }

    return circuitState.get(targetKey);
}

function canPassCircuit(targetKey) {
    const entry = getCircuitEntry(targetKey);
    if (entry.state !== "OPEN") {
        return true;
    }

    if (Date.now() - entry.openedAt >= CIRCUIT_OPEN_MS) {
        entry.state = "HALF_OPEN";
        return true;
    }

    return false;
}

function recordSuccess(targetKey, latencyMs) {
    const bucket = getMetricBucket(targetKey);
    const circuit = getCircuitEntry(targetKey);
    metrics.totals.successes += 1;
    bucket.successes += 1;
    bucket.totalLatencyMs += latencyMs;
    bucket.lastSuccessAt = new Date().toISOString();
    circuit.failures = 0;
    circuit.state = "CLOSED";
    circuit.openedAt = 0;
}

function recordFailure(targetKey, errorMessage) {
    const bucket = getMetricBucket(targetKey);
    const circuit = getCircuitEntry(targetKey);
    metrics.totals.failures += 1;
    bucket.failures += 1;
    bucket.lastError = errorMessage;
    circuit.failures += 1;

    if (circuit.failures >= CIRCUIT_FAILURE_THRESHOLD) {
        circuit.state = "OPEN";
        circuit.openedAt = Date.now();
    }
}

async function send(optionsOrUrl, data, legacyOptions = {}) {
    const options = typeof optionsOrUrl === "string"
        ? { url: optionsOrUrl, data, ...legacyOptions }
        : { ...optionsOrUrl };
    const method = (options.method || "POST").toUpperCase();
    const url = options.url;
    const timeout = options.timeout || DEFAULT_TIMEOUT_MS;
    const retries = Number.isInteger(options.retries) ? options.retries : DEFAULT_RETRIES;
    const requestId =
        options.requestId ||
        getRequestIdFromHeaders(options.headers) ||
        createRequestId(options.sourcePort || "node");
    const targetKey = getTargetKey(method, url);
    const bucket = getMetricBucket(targetKey);

    metrics.totals.requests += 1;
    bucket.requests += 1;
    bucket.lastRequestAt = new Date().toISOString();

    if (!canPassCircuit(targetKey)) {
        metrics.totals.circuitOpenRejects += 1;
        bucket.circuitOpenRejects += 1;
        const error = new Error(`Circuit open for ${targetKey}`);
        logToFileOnly("WARN", `[Sidecar Circuit Open] requestId=${requestId} target=${targetKey}`);
        throw error;
    }

    let attempt = 0;
    let lastError = null;

    while (attempt <= retries) {
        attempt += 1;
        const startedAt = Date.now();

        logToFileOnly(
            "LOG",
            `[Sidecar Outbound Request] ts=${new Date().toISOString()} requestId=${requestId} method=${method} url=${url} attempt=${attempt} body=${serializeForLog(options.data)}`
        );

        try {
            const response = await axios({
                method,
                url,
                data: options.data,
                timeout,
                headers: {
                    "x-request-id": requestId,
                    ...(options.headers || {}),
                },
            });

            const latencyMs = Date.now() - startedAt;
            recordSuccess(targetKey, latencyMs);

            logToFileOnly(
                "LOG",
                `[Sidecar Outbound Response] ts=${new Date().toISOString()} requestId=${requestId} method=${method} url=${url} status=${response.status} durationMs=${latencyMs} body=${serializeForLog(response.data)}`
            );

            return response.data;
        } catch (err) {
            lastError = err;
            recordFailure(targetKey, err.message);

            logToFileOnly(
                "WARN",
                `[Sidecar Failure] ts=${new Date().toISOString()} requestId=${requestId} method=${method} url=${url} attempt=${attempt} error=${err.message}`
            );

            if (attempt <= retries) {
                metrics.totals.retries += 1;
                bucket.retries += 1;
                logToFileOnly(
                    "LOG",
                    `[Sidecar Retry] requestId=${requestId} method=${method} url=${url} nextAttempt=${attempt + 1}`
                );
                continue;
            }
        }
    }

    throw lastError;
}

function post(url, data, options = {}) {
    return send({ ...options, method: "POST", url, data });
}

function get(url, options = {}) {
    return send({ ...options, method: "GET", url });
}

function getMetrics() {
    const byTarget = Object.entries(metrics.byTarget).reduce((acc, [target, value]) => {
        const circuit = getCircuitEntry(target);
        acc[target] = {
            ...value,
            averageLatencyMs: value.successes ? Number((value.totalLatencyMs / value.successes).toFixed(2)) : 0,
            circuitState: circuit.state,
            consecutiveFailures: circuit.failures,
            circuitOpenedAt: circuit.openedAt ? new Date(circuit.openedAt).toISOString() : null,
        };
        return acc;
    }, {});

    return {
        totals: { ...metrics.totals },
        byTarget,
    };
}

module.exports = { send, post, get, getMetrics };
