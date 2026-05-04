const axios = require("axios");
const { logToFileOnly, serializeForLog } = require("../utils/logger");

async function send(url, data) {
    const start = Date.now();
    logToFileOnly("LOG", `[Proxy Request] ${url} body=${serializeForLog(data)}`);

    try {
        const res = await axios.post(url, data, { timeout: 3000 });
        // console.log(`[Proxy Response] ${url} status=${res.status} durationMs=${Date.now() - start} body=${serializeForLog(res.data)}`);
        return res.data;
    } catch (err) {
        logToFileOnly("LOG", `[Proxy Retry] ${url} reason=${err.message}`);
        const retry = await axios.post(url, data, { timeout: 3000 });
        // console.log(`[Proxy Response] ${url} status=${retry.status} durationMs=${Date.now() - start} body=${serializeForLog(retry.data)}`);
        return retry.data;
    }
}

module.exports = { send };
