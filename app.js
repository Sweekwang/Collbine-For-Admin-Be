// Main application file for Collbine Admin

const express = require('express');
const cors = require('cors');
const app = express();

// Middleware
app.use(cors({
  origin: 'http://localhost:3000',
  credentials: true
}));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Routes
const feedRoutes = require('./routes/routes');
app.use('/api', feedRoutes);


// Start the server
app.listen(8080, () => {
  console.log('Server running on port 8080');
});

