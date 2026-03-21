const express = require('express');
const app = express();
const port = 8001;

// Define a basic route
app.get('/', (req, res) => {
    res.send('Hello World! Aggregator');
});

// Start the server
app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
});