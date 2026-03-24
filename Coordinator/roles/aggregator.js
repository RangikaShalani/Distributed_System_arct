let finalResult = {};
let processed = new Set();

module.exports = (req, res) => {
    const { result, taskId } = req.body;

    if (processed.has(taskId)) return res.send("Duplicate");

    processed.add(taskId);

    for (let key in result) {
        if (!finalResult[key]) finalResult[key] = 0;
        finalResult[key] += result[key].count;
    }

    console.log("FINAL RESULT:", finalResult);

    res.send("Aggregated");
};