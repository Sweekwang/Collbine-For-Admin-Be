// Controller for Collbine endpoints

require('dotenv').config();
const asyncHandler = require('../utils/asyncHandler');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient } = require('@aws-sdk/lib-dynamodb');
const { ScanCommand } = require('@aws-sdk/lib-dynamodb');

// Initialize DynamoDB client
const client = new DynamoDBClient({ region: 'ap-southeast-1' });

// Create DynamoDB Document Client for easier data handling
const dynamoDB = DynamoDBDocumentClient.from(client);

// Get all items from review_customer_release table
exports.reviewCustomerRelease = asyncHandler(async (req, res, next) => {
  const TABLE_NAME = 'review_customer_release';
  const { Items } = await dynamoDB.send(
    new ScanCommand({
      TableName: TABLE_NAME,
    })
  );
  
  res.status(200).json({
    success: true,
    count: Items ? Items.length : 0,
    data: Items || []
  });
});


