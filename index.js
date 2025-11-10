// Main entry point for the Collbine Admin application

const app = require('./app');

const PORT = process.env.PORT || 3000;

// Start the server
app.listen(PORT, () => {
  console.log(`Collbine Admin server is running on port ${PORT}`);
});

