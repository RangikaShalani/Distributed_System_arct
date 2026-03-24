const axios = require("axios");

async function send(url, data) {
    const start = Date.now();

    try {
        const res = await axios.post(url, data);

        console.log(`[Proxy] ${url} (${Date.now() - start}ms)`);

        return res.data;
    } catch (err) {
        console.log(`[Retry] ${url}`);
        return axios.post(url, data);
    }
}

module.exports = { send };