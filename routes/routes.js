// Routes file for Collbine Admin

const express = require('express');
const router = express.Router();
const collbineEndpoints = require('../controller/collbineEndpoints');
const asyncHandler = require('../utils/asyncHandler');


// Review Customer Release routes
router.get('/review-customer-release', collbineEndpoints.reviewCustomerRelease);



// Export the router
module.exports = router;

