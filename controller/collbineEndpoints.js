// Controller for Collbine endpoints

require('dotenv').config();
const asyncHandler = require('../utils/asyncHandler');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient } = require('@aws-sdk/lib-dynamodb');
const { ScanCommand, QueryCommand, PutCommand, DeleteCommand } = require('@aws-sdk/lib-dynamodb');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');

// Initialize DynamoDB client
const client = new DynamoDBClient({ region: 'ap-southeast-1' });

// Create DynamoDB Document Client for easier data handling
const dynamoDB = DynamoDBDocumentClient.from(client);

// Initialize S3 client
const s3Client = new S3Client({ region: 'ap-southeast-1' });

// Helper function to extract bucket and key from S3 URL
function parseS3Url(url) {
  if (!url || typeof url !== 'string') return null;
  
  // Handle s3://bucket/key format
  if (url.startsWith('s3://')) {
    const parts = url.replace('s3://', '').split('/');
    return {
      bucket: parts[0],
      key: parts.slice(1).join('/')
    };
  }
  
  // Handle https://bucket.s3.region.amazonaws.com/key format
  const s3UrlPattern = /https?:\/\/([^.]+)\.s3[.-]([^.]+)\.amazonaws\.com\/(.+)/;
  const match = url.match(s3UrlPattern);
  if (match) {
    return {
      bucket: match[1],
      key: match[3]
    };
  }
  
  // Handle https://s3.region.amazonaws.com/bucket/key format
  const s3PathPattern = /https?:\/\/s3[.-]([^.]+)\.amazonaws\.com\/([^\/]+)\/(.+)/;
  const pathMatch = url.match(s3PathPattern);
  if (pathMatch) {
    return {
      bucket: pathMatch[2],
      key: pathMatch[3]
    };
  }
  
  return null;
}

// Helper function to presign S3 URL
async function presignS3Url(url, expiresIn = 3600) {
  try {
    const s3Params = parseS3Url(url);
    if (!s3Params) return url; // Return original URL if not a valid S3 URL
    
    const command = new GetObjectCommand({
      Bucket: s3Params.bucket,
      Key: s3Params.key
    });
    
    const presignedUrl = await getSignedUrl(s3Client, command, { expiresIn });
    return presignedUrl;
  } catch (error) {
    console.error('Error presigning S3 URL:', error);
    return url; // Return original URL on error
  }
}

// Recursive function to find and presign image URLs in data
async function presignImageUrls(data) {
  if (Array.isArray(data)) {
    return Promise.all(data.map(item => presignImageUrls(item)));
  } else if (data && typeof data === 'object') {
    const result = {};
    for (const [key, value] of Object.entries(data)) {
      // Check if the value is a string that looks like an S3 URL or image URL
      if (typeof value === 'string' && (
        value.startsWith('s3://') || 
        value.includes('.s3.') || 
        value.includes('amazonaws.com') ||
        // Also check for image-related keys with URLs that might be S3
        (key.toLowerCase().includes('image') || 
         key.toLowerCase().includes('photo') || 
         key.toLowerCase().includes('logo') ||
         key.toLowerCase().includes('url')) && 
        (value.includes('s3') || value.includes('amazonaws') || parseS3Url(value))
      )) {
        // Only presign if it's actually an S3 URL
        const s3Params = parseS3Url(value);
        if (s3Params) {
          result[key] = await presignS3Url(value);
        } else {
          result[key] = value; // Keep original if not S3 URL
        }
      } else {
        result[key] = await presignImageUrls(value);
      }
    }
    return result;
  }
  return data;
}

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
  
  // Query all 5 tables in parallel using QueryCommand (shop_id is partition key)
  const [businessInfoResult, cardDesignResult, customerFacingResult, stampDataResult, shopReleaseContactResult] = await Promise.all([
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
    ),
    // shop_release_contact table
    dynamoDB.send(
      new QueryCommand({
        TableName: 'shop_release_contact',
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
    StampData: stampDataResult.Items || [],
    shop_release_contact: shopReleaseContactResult.Items || []
  };
  
  // Presign all image URLs in the data
  const presignedData = await presignImageUrls(combinedData);
  
  res.status(200).json({
    success: true,
    shop_id: shop_id,
    data: presignedData
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
  
  // Get data from both review_customer_release and shop_release_contact in parallel
  const [reviewResult, shopReleaseContactResult] = await Promise.all([
    dynamoDB.send(
      new QueryCommand({
        TableName: 'review_customer_release',
        KeyConditionExpression: 'shop_id = :shop_id',
        ExpressionAttributeValues: {
          ':shop_id': shop_id
        }
      })
    ),
    dynamoDB.send(
      new QueryCommand({
        TableName: 'shop_release_contact',
        KeyConditionExpression: 'shop_id = :shop_id',
        ExpressionAttributeValues: {
          ':shop_id': shop_id
        }
      })
    )
  ]);
  
  if (!reviewResult.Items || reviewResult.Items.length === 0) {
    return res.status(404).json({
      success: false,
      error: 'No record found in review_customer_release for the given shop_id'
    });
  }
  
  // Get data from review_customer_release
  const reviewItem = reviewResult.Items[0];
  const { shop_id: retrievedShopId, sentdatetime } = reviewItem;
  
  if (!sentdatetime) {
    return res.status(400).json({
      success: false,
      error: 'sentdatetime not found in the review record'
    });
  }
  
  // Get data from shop_release_contact (if exists)
  const shopReleaseContactItem = shopReleaseContactResult.Items && shopReleaseContactResult.Items.length > 0 
    ? shopReleaseContactResult.Items[0] 
    : {};
  
  // Combine data from both tables
  const combinedData = {
    ...reviewItem,
    ...shopReleaseContactItem,
    shop_id: retrievedShopId,
    sentdatetime: sentdatetime,
    accepted_at: new Date().toISOString()
  };
  
  // Check if release_type is "scheduled" from shop_release_contact
  const releaseType = shopReleaseContactItem.release_type;
  const isScheduled = releaseType === 'scheduled';
  
  // Store in appropriate table based on release_type
  if (isScheduled) {
    // Store in Scheduled_Accepted_Customer_Review
    await dynamoDB.send(
      new PutCommand({
        TableName: 'Scheduled_Accepted_Customer_Review',
        Item: combinedData
      })
    );
  } else {
    // Store in Accepted_Customer_Review
    await dynamoDB.send(
      new PutCommand({
        TableName: 'Accepted_Customer_Review',
        Item: combinedData
      })
    );
  }
  
  // Store in ReleaseHistory for audit trail
  // Primary key: shop_id, Sort key: accepted_at (datetime when accepted)
  await dynamoDB.send(
    new PutCommand({
      TableName: 'ReleaseHistory',
      Item: {
        shop_id: retrievedShopId,
        accepted_at: combinedData.accepted_at, // Sort key (datetime)
        ...combinedData // Include all combined data
      }
    })
  );
  
  // Delete from review_customer_release
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
    message: isScheduled 
      ? 'Customer review accepted and stored in Scheduled_Accepted_Customer_Review'
      : 'Customer review accepted and stored in Accepted_Customer_Review',
    data: {
      shop_id: retrievedShopId,
      sentdatetime: sentdatetime,
      release_type: releaseType || 'manual',
      table: isScheduled ? 'Scheduled_Accepted_Customer_Review' : 'Accepted_Customer_Review'
    }
  });
});

