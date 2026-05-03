const fs = require("fs");

module.exports = function readChunk({ chunk, sourceFilePath, startLine, endLine }) {
    if (Array.isArray(chunk) && chunk.length > 0) {
        return chunk;
    }

    if (!sourceFilePath) {
        return [];
    }

    const file = fs.readFileSync(sourceFilePath, "utf-8");
    const lines = file.split(/\r?\n/).filter(Boolean);
    const startIndex = Math.max(0, Number(startLine || 1) - 1);
    const endIndex = Math.max(startIndex, Number(endLine || startIndex + 1));

    return lines.slice(startIndex, endIndex);
};
