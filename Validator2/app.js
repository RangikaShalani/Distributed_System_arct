const express = require('express');
const app = express();
const port = 8006;

// Define a basic route
app.get('/', (req, res) => {
    res.send('Hello World! Validator 2');
});

// Start the server
app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
});