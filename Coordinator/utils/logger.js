const fs = require("fs");
const path = require("path");
const util = require("util");

let initializedPort = null;
let activeLogWriter = null;
const MAX_LOG_VALUE_LENGTH = 4000;
let requestCounter = 0;

function serializeForLog(value) {
    if (value === undefined) {
        return "undefined";
    }

    const normalized = typeof value === "string"
        ? value
        : util.inspect(value, { depth: 5, breakLength: 120, maxArrayLength: 50 });

    if (normalized.length <= MAX_LOG_VALUE_LENGTH) {
        return normalized;
    }

    return `${normalized.slice(0, MAX_LOG_VALUE_LENGTH)}... [truncated ${normalized.length - MAX_LOG_VALUE_LENGTH} chars]`;
}

function initNodeLogger(port) {
    if (initializedPort) {
        return;
    }

    const resolvedPort = String(port || "unknown");
    const logsDir = path.resolve(__dirname, "..", "sidecar_logs");
    fs.mkdirSync(logsDir, { recursive: true });

    const logFilePath = path.join(logsDir, `${resolvedPort}.log`);
    const stream = fs.createWriteStream(logFilePath, { flags: "a" });

    const originalConsole = {
        log: console.log.bind(console),
        info: console.info.bind(console),
        warn: console.warn.bind(console),
        error: console.error.bind(console),
    };

    const writeLog = (level, args) => {
        const timestamp = new Date().toISOString();
        const message = util.format(...args);
        stream.write(`[${timestamp}] [${level}] ${message}\n`);
    };

    activeLogWriter = writeLog;

    console.log = (...args) => {
        writeLog("LOG", args);
        originalConsole.log(...args);
    };

    console.info = (...args) => {
        writeLog("INFO", args);
        originalConsole.info(...args);
    };

    console.warn = (...args) => {
        writeLog("WARN", args);
        originalConsole.warn(...args);
    };

    console.error = (...args) => {
        writeLog("ERROR", args);
        originalConsole.error(...args);
    };

    process.on("uncaughtException", err => {
        writeLog("ERROR", ["Uncaught exception:", err && err.stack ? err.stack : err]);
        originalConsole.error(err);
    });

    process.on("unhandledRejection", reason => {
        writeLog("ERROR", ["Unhandled rejection:", reason]);
        originalConsole.error(reason);
    });

    initializedPort = resolvedPort;
    console.log(`Sidecar log file initialized at ${logFilePath}`);
}

function createRequestId(portHint = initializedPort || "node") {
    requestCounter += 1;
    return `${portHint}-${Date.now()}-${requestCounter}`;
}

function getRequestIdFromHeaders(headers = {}) {
    return headers["x-request-id"] || headers["X-Request-Id"] || null;
}

function logToFileOnly(level, ...args) {
    if (typeof activeLogWriter === "function") {
        activeLogWriter(level, args);
        return;
    }

    console[level === "ERROR" ? "error" : "log"](...args);
}

function requestResponseLogger(req, res, next) {
    const start = Date.now();
    let responseLogged = false;
    const requestId = getRequestIdFromHeaders(req.headers) || createRequestId();
    req.requestId = requestId;
    req.requestReceivedAt = new Date().toISOString();
    res.setHeader("x-request-id", requestId);

    logToFileOnly(
        "LOG",
        `[Inbound Request] ts=${req.requestReceivedAt} requestId=${requestId} method=${req.method} path=${req.originalUrl}`
    );

    const originalJson = res.json.bind(res);
    const originalSend = res.send.bind(res);
    const logResponse = body => {
        if (responseLogged) {
            return;
        }

        responseLogged = true;
        logToFileOnly(
            "LOG",
            `[Outbound Response] ts=${new Date().toISOString()} requestId=${requestId} method=${req.method} path=${req.originalUrl} status=${res.statusCode} durationMs=${Date.now() - start}`
        );
    };

    res.json = body => {
        logResponse(body);
        return originalJson(body);
    };

    res.send = body => {
        logResponse(body);
        return originalSend(body);
    };

    next();
}

module.exports = {
    createRequestId,
    getRequestIdFromHeaders,
    initNodeLogger,
    logToFileOnly,
    requestResponseLogger,
    serializeForLog,
};
