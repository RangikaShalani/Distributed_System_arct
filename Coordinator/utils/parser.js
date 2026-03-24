module.exports = function parse(line) {
    if (!line) return "UNKNOWN";

    if (line.includes("FATAL")) return "CRITICAL";
    if (line.includes("ERROR")) return "ERROR";
    if (line.includes("WARNING")) return "WARNING";
    if (line.includes("INFO")) return "INFO";
    if (line.includes("DEBUG")) return "DEBUG";

    return "INFO";
};