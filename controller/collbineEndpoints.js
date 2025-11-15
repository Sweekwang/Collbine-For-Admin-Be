// Controller for Collbine endpoints

require('dotenv').config();
const asyncHandler = require('../utils/asyncHandler');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient } = require('@aws-sdk/lib-dynamodb');
const { ScanCommand, QueryCommand, PutCommand, DeleteCommand } = require('@aws-sdk/lib-dynamodb');

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

exports.fullIndividualCustomerDetails = asyncHandler(async (req, res, next) => {
  // Support both GET (query params) and POST (body)
  const shop_id = req.body.shop_id || req.query.shop_id;
  
  if (!shop_id) {
    return res.status(400).json({
      success: false,
      error: 'shop_id is required (use query parameter for GET or body for POST)'
    });
  }
  
  // Query all 4 tables in parallel using QueryCommand (shop_id is partition key)
  const [businessInfoResult, cardDesignResult, customerFacingResult, stampDataResult] = await Promise.all([
    // businessinformations table
    dynamoDB.send(
      new QueryCommand({
        TableName: 'businessinformations',
        KeyConditionExpression: 'shop_id = :shop_id',
        ExpressionAttributeValues: {
          ':shop_id': shop_id
        }
      })
    ),
    // Card_Design table
    dynamoDB.send(
      new QueryCommand({
        TableName: 'Card_Design',
        KeyConditionExpression: 'shop_id = :shop_id',
        ExpressionAttributeValues: {
          ':shop_id': shop_id
        }
      })
    ),
    // CustomerFacingDetails table
    dynamoDB.send(
      new QueryCommand({
        TableName: 'CustomerFacingDetails',
        KeyConditionExpression: 'shop_id = :shop_id',
        ExpressionAttributeValues: {
          ':shop_id': shop_id
        }
      })
    ),
    // StampData table
    dynamoDB.send(
      new QueryCommand({
        TableName: 'StampData',
        KeyConditionExpression: 'shop_id = :shop_id',
        ExpressionAttributeValues: {
          ':shop_id': shop_id
        }
      })
    )
  ]);
  
  // Combine all results
  const combinedData = {
    businessinformations: businessInfoResult.Items || [],
    Card_Design: cardDesignResult.Items || [],
    CustomerFacingDetails: customerFacingResult.Items || [],
    StampData: stampDataResult.Items || []
  };
  
  res.status(200).json({
    success: true,
    shop_id: shop_id,
    data: combinedData
  });
});

exports.reject_customer_review = asyncHandler(async (req, res, next) => {
  const { shop_id, reason } = req.body;
  
  if (!shop_id) {
    return res.status(400).json({
      success: false,
      error: 'shop_id is required in request body'
    });
  }
  
  if (!reason || typeof reason !== 'string' || reason.trim().length === 0) {
    return res.status(400).json({
      success: false,
      error: 'reason is required and must be a non-empty string'
    });
  }
  
  // Get the item from review_customer_release
  const reviewResult = await dynamoDB.send(
    new QueryCommand({
      TableName: 'review_customer_release',
      KeyConditionExpression: 'shop_id = :shop_id',
      ExpressionAttributeValues: {
        ':shop_id': shop_id
      }
    })
  );
  
  if (!reviewResult.Items || reviewResult.Items.length === 0) {
    return res.status(404).json({
      success: false,
      error: 'No record found in review_customer_release for the given shop_id'
    });
  }
  
  // Get shop_id and sentdatetime from the first item
  const reviewItem = reviewResult.Items[0];
  const { shop_id: retrievedShopId, sentdatetime } = reviewItem;
  
  if (!sentdatetime) {
    return res.status(400).json({
      success: false,
      error: 'sentdatetime not found in the review record'
    });
  }
  
  // Insert into Rejected_Customer_Review
  await dynamoDB.send(
    new PutCommand({
      TableName: 'Rejected_Customer_Review',
      Item: {
        shop_id: retrievedShopId,
        sentdatetime: sentdatetime,
        reason: reason,
        rejected_at: new Date().toISOString()
      }
    })
  );
  
  // Delete from review_customer_release using shop_id from body
  await dynamoDB.send(
    new DeleteCommand({
      TableName: 'review_customer_release',
      Key: {
        shop_id: shop_id
      }
    })
  );
  
  res.status(200).json({
    success: true,
    message: 'Customer review rejected and moved to Rejected_Customer_Review',
    data: {
      shop_id: retrievedShopId,
      sentdatetime: sentdatetime,
      reason: reason
    }
  });
});

exports.acceptinvitation = asyncHandler(async (req, res, next) => {
  const { shop_id } = req.body;
  
  if (!shop_id) {
    return res.status(400).json({
      success: false,
      error: 'shop_id is required in request body'
    });
  }
  
  // Get the item from review_customer_release
  const reviewResult = await dynamoDB.send(
    new QueryCommand({
      TableName: 'review_customer_release',
      KeyConditionExpression: 'shop_id = :shop_id',
      ExpressionAttributeValues: {
        ':shop_id': shop_id
      }
    })
  );
  
  if (!reviewResult.Items || reviewResult.Items.length === 0) {
    return res.status(404).json({
      success: false,
      error: 'No record found in review_customer_release for the given shop_id'
    });
  }
  
  // Get shop_id and sentdatetime from the first item
  const reviewItem = reviewResult.Items[0];
  const { shop_id: retrievedShopId, sentdatetime } = reviewItem;
  
  if (!sentdatetime) {
    return res.status(400).json({
      success: false,
      error: 'sentdatetime not found in the review record'
    });
  }
  
  // Insert into Accepted_Customer_Review
  await dynamoDB.send(
    new PutCommand({
      TableName: 'Accepted_Customer_Review',
      Item: {
        shop_id: retrievedShopId,
        sentdatetime: sentdatetime,
        accepted_at: new Date().toISOString()
      }
    })
  );
  
  // Delete from review_customer_release using shop_id from body
  await dynamoDB.send(
    new DeleteCommand({
      TableName: 'review_customer_release',
      Key: {
        shop_id: shop_id
      }
    })
  );
  
  res.status(200).json({
    success: true,
    message: 'Customer review accepted and moved to Accepted_Customer_Review',
    data: {
      shop_id: retrievedShopId,
      sentdatetime: sentdatetime
    }
  });
});

