// Routes file for Collbine Admin

const express = require('express');
const router = express.Router();
const collbineEndpoints = require('../controller/collbineEndpoints');
const asyncHandler = require('../utils/asyncHandler');


// Review Customer Release routes
router.get('/review-customer-release', collbineEndpoints.reviewCustomerRelease);

router.get('/fullIndividualCustomerDetails', collbineEndpoints.fullIndividualCustomerDetails);

router.post('/reject_customer_review', collbineEndpoints.reject_customer_review);

router.post('/acceptinvitation', collbineEndpoints.acceptinvitation);

router.get('/release-history', collbineEndpoints.getReleaseHistory);

// Export the router
module.exports = router;

