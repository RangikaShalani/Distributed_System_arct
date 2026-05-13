const parse = require("./parser");

module.exports = function summarizeChunk(chunk = []) {
    const summary = {};

    for (const line of chunk) {
        const severity = parse(line);

        if (!summary[severity]) {
            summary[severity] = {
                count: 0,
                messages: new Set(),
            };
        }

        summary[severity].count += 1;
        summary[severity].messages.add(line);
    }

    return Object.fromEntries(
        Object.entries(summary).map(([severity, value]) => [
            severity,
            {
                count: value.count,
                messages: Array.from(value.messages).sort(),
            },
        ])
    );
};
