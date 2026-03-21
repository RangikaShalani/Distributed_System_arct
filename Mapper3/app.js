const express = require('express');
const app = express();
const port = 8004;

// Define a basic route
app.get('/', (req, res) => {
    res.send('Hello World! Mapper 4');
});

// Start the server
app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
});