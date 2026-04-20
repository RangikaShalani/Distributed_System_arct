const axios = require("axios");

async function send(url, data) {
    const start = Date.now();

    try {
        const res = await axios.post(url, data, { timeout: 3000 });
        console.log(`[Proxy] ${url} (${Date.now() - start}ms)`);
        return res.data;
    } catch (err) {
        console.log(`[Retry] ${url}`);
        const retry = await axios.post(url, data, { timeout: 3000 });
        return retry.data;
    }
}

module.exports = { send };
