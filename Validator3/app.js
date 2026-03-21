const express = require('express');
const app = express();
const port = 8007;

// Define a basic route
app.get('/', (req, res) => {
    res.send('Hello World! Validator 3');
});

// Start the server
app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
});