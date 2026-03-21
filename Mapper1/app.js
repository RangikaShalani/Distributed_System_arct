const express = require('express');
const app = express();
const port = 8002;

// Define a basic route
app.get('/', (req, res) => {
    res.send('Hello World! Mapper 1');
});

// Start the server
app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
});