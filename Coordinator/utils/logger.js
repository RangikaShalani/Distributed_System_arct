const fs = require("fs");
const path = require("path");
const util = require("util");

let initializedPort = null;
const MAX_LOG_VALUE_LENGTH = 4000;

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

function requestResponseLogger(req, res, next) {
    const start = Date.now();
    let responseLogged = false;
    console.log(`[Inbound Request] ${req.method} ${req.originalUrl} body=${serializeForLog(req.body)}`);

    const originalJson = res.json.bind(res);
    const originalSend = res.send.bind(res);
    const logResponse = body => {
        if (responseLogged) {
            return;
        }

        responseLogged = true;
        console.log(
            `[Outbound Response] ${req.method} ${req.originalUrl} status=${res.statusCode} durationMs=${Date.now() - start} body=${serializeForLog(body)}`
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
    initNodeLogger,
    requestResponseLogger,
    serializeForLog,
};
