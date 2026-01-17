// Controller for Collbine endpoints

require('dotenv').config();
const asyncHandler = require('../utils/asyncHandler');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient } = require('@aws-sdk/lib-dynamodb');
const { ScanCommand, QueryCommand, PutCommand, DeleteCommand, UpdateCommand } = require('@aws-sdk/lib-dynamodb');
const { S3Client, GetObjectCommand, CopyObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');

// Initialize DynamoDB client
const client = new DynamoDBClient({ region: 'ap-southeast-1' });

// Create DynamoDB Document Client for easier data handling
const dynamoDB = DynamoDBDocumentClient.from(client);

// Initialize S3 client
const s3Client = new S3Client({ region: 'ap-southeast-1' });

// Initialize Supabase client (service role for server-side use)
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
const supabase =
  supabaseUrl && supabaseServiceRoleKey
    ? createClient(supabaseUrl, supabaseServiceRoleKey)
    : null;

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

// Helper function to unmarshal DynamoDB format to plain JavaScript objects
function unmarshalDynamoDBItem(item) {
  if (item === null || item === undefined) {
    return item;
  }

  // Handle DynamoDB Map type { "M": {...} }
  if (item.M && typeof item.M === 'object') {
    const result = {};
    for (const [key, value] of Object.entries(item.M)) {
      result[key] = unmarshalDynamoDBItem(value);
    }
    return result;
  }

  // Handle DynamoDB String type { "S": "..." }
  if (item.S !== undefined) {
    return item.S;
  }

  // Handle DynamoDB Number type { "N": "123" }
  if (item.N !== undefined) {
    const num = parseFloat(item.N);
    return isNaN(num) ? item.N : num;
  }

  // Handle DynamoDB Boolean type { "BOOL": true }
  if (item.BOOL !== undefined) {
    return item.BOOL;
  }

  // Handle DynamoDB List type { "L": [...] }
  if (item.L && Array.isArray(item.L)) {
    return item.L.map(element => unmarshalDynamoDBItem(element));
  }

  // Handle DynamoDB Null type { "NULL": true }
  if (item.NULL !== undefined) {
    return null;
  }

  // Handle DynamoDB Binary type { "B": Buffer }
  if (item.B !== undefined) {
    return item.B;
  }

  // Handle DynamoDB String Set { "SS": [...] }
  if (item.SS && Array.isArray(item.SS)) {
    return item.SS;
  }

  // Handle DynamoDB Number Set { "NS": [...] }
  if (item.NS && Array.isArray(item.NS)) {
    return item.NS.map(n => parseFloat(n));
  }

  // If it's an array, recursively process each element
  if (Array.isArray(item)) {
    return item.map(element => unmarshalDynamoDBItem(element));
  }

  // If it's a plain object, recursively process each property
  if (typeof item === 'object') {
    const result = {};
    for (const [key, value] of Object.entries(item)) {
      result[key] = unmarshalDynamoDBItem(value);
    }
    return result;
  }

  // Return as-is if it's already a plain value
  return item;
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

// Helper to copy an image from the private upload bucket to the public live bucket (keeps original in source bucket)
async function moveImageToLiveBucket(imageUrl) {
  if (!imageUrl) return null;
  
  const src = parseS3Url(imageUrl);
  if (!src) {
    console.warn(`Could not parse S3 URL: ${imageUrl}`);
    return null;
  }

  const targetBucket = 'collbine-shop-images-live';

  // If already in the target bucket, just return a public URL
  if (src.bucket === targetBucket) {
    return `https://${targetBucket}.s3.ap-southeast-1.amazonaws.com/${src.key}`;
  }

  try {
    console.log(`Copying image from ${src.bucket}/${src.key} to ${targetBucket}/${src.key}`);
    
    // Copy to the public bucket with the same key (keep original in source bucket)
    await s3Client.send(
      new CopyObjectCommand({
        Bucket: targetBucket,
        Key: src.key,
        CopySource: `${src.bucket}/${src.key}`
        // ACL omitted because target bucket enforces bucket-owner access (no ACLs)
      })
    );
    console.log(`Successfully copied image to ${targetBucket}/${src.key}`);

    // Return the new public URL
    const publicUrl = `https://${targetBucket}.s3.ap-southeast-1.amazonaws.com/${src.key}`;
    return publicUrl;
  } catch (err) {
    // If the source key doesn't exist, check if it's already in the target bucket
    if (err.Code === 'NoSuchKey' || err.name === 'NoSuchKey') {
      console.log(`Image not found in source bucket ${src.bucket}/${src.key}, checking target bucket...`);
      try {
        // Check if image already exists in target bucket
        await s3Client.send(
          new HeadObjectCommand({
            Bucket: targetBucket,
            Key: src.key
          })
        );
        // Image exists in target bucket, return public URL
        console.log(`Image already exists in target bucket, using existing URL`);
        return `https://${targetBucket}.s3.ap-southeast-1.amazonaws.com/${src.key}`;
      } catch (headErr) {
        // Image doesn't exist in either bucket
        console.warn(`Image not found in source or target bucket: ${src.key}`);
        return null;
      }
    }
    
    // For other errors, log and return null
    console.error(`Error moving image to live bucket (${imageUrl}):`, err.message);
    return null;
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

// Helper function to geocode address to latitude and longitude
// For Singapore addresses, use only postal code + country for faster/more reliable lookup
async function geocodeAddress(addressUnitNumber, addressStreetAddress, addressBuildingName, addressPostalCode, retries = 3) {
  const addressParts = [];
  if (addressPostalCode && addressPostalCode.trim() !== '') {
    addressParts.push(addressPostalCode.trim());
  }
  addressParts.push('Singapore');

  const fullAddress = addressParts.join(' ');

  if (!fullAddress.trim()) {
    throw new Error('Address is empty');
  }
  
  // Use OpenStreetMap Nominatim API (free, no API key required)
  // Note: Add a delay to respect rate limits (1 request per second recommended)
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const response = await axios.get('https://nominatim.openstreetmap.org/search', {
        params: {
          q: fullAddress,
          format: 'json',
          limit: 1,
          addressdetails: 1
        },
        headers: {
          'User-Agent': 'Collbine-Admin-App' // Required by Nominatim
        },
        timeout: 30000 // 30 second timeout (increased from 10s)
      });
      
      if (!response.data || response.data.length === 0) {
        throw new Error(`No geocoding results found for address: ${fullAddress}`);
      }
      
      const result = response.data[0];
      return {
        latitude: parseFloat(result.lat),
        longitude: parseFloat(result.lon)
      };
    } catch (error) {
      const isTimeout = error.code === 'ECONNABORTED' || error.message.includes('timeout');
      const isLastAttempt = attempt === retries;
      
      if (isLastAttempt) {
        throw new Error(`Geocoding failed after ${retries} attempts: ${error.message}`);
      }
      
      if (isTimeout) {
        // Exponential backoff: wait 2^attempt seconds before retry
        const waitTime = Math.pow(2, attempt) * 1000;
        console.warn(`Geocoding timeout for "${fullAddress}" (attempt ${attempt}/${retries}). Retrying in ${waitTime/1000}s...`);
        await new Promise(resolve => setTimeout(resolve, waitTime));
      } else {
        // For non-timeout errors, retry immediately or with shorter delay
        console.warn(`Geocoding error for "${fullAddress}" (attempt ${attempt}/${retries}): ${error.message}. Retrying...`);
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
  }
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

// Get release history for a shop (shop_id from cookie)
exports.getReleaseHistory = asyncHandler(async (req, res, next) => {
  const rawCookie = req.headers?.cookie;
  let shop_id = null;
  
  if (rawCookie && typeof rawCookie === 'string') {
    const cookies = rawCookie.split(';').map(cookie => cookie.trim());
    const target = cookies.find(cookie => cookie.startsWith('shop_id='));
    if (target) {
      shop_id = decodeURIComponent(target.substring('shop_id='.length));
    }
  }
  
  if (!shop_id) {
    return res.status(400).json({
      success: false,
      error: 'shop_id cookie is required'
    });
  }
  
  const historyResult = await dynamoDB.send(
    new QueryCommand({
      TableName: 'ReleaseHistory',
      KeyConditionExpression: 'shop_id = :shop_id',
      ExpressionAttributeValues: {
        ':shop_id': shop_id
      }
    })
  );
  
  res.status(200).json({
    success: true,
    count: historyResult.Items ? historyResult.Items.length : 0,
    shop_id: shop_id,
    data: historyResult.Items || []
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
  
  const rejectedAt = new Date().toISOString();
  
  // Insert into Rejected_Customer_Review
  await dynamoDB.send(
    new PutCommand({
      TableName: 'Rejected_Customer_Review',
      Item: {
        shop_id: retrievedShopId,
        sentdatetime: sentdatetime,
        reason: reason,
        rejected_at: rejectedAt
      }
    })
  );
  
  // Update review_status to "rejected" and review_time timestamp in shop_release_contact
  await dynamoDB.send(
    new UpdateCommand({
      TableName: 'shop_release_contact',
      Key: {
        shop_id: shop_id
      },
      UpdateExpression: 'SET review_status = :status, review_time = :reviewTime',
      ExpressionAttributeValues: {
        ':status': 'rejected',
        ':reviewTime': rejectedAt
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
      reason: reason,
      rejected_at: rejectedAt
    }
  });
});

exports.acceptinvitation = asyncHandler(async (req, res, next) => {
  const { shop_id } = req.body;
  console.log('shop_id', shop_id);
  
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
  
  // Query the 4 tables to include in ReleaseHistory
  const [businessInfoResult, cardDesignResult, customerFacingResult, stampDataResult] = await Promise.all([
    dynamoDB.send(
      new QueryCommand({
        TableName: 'businessinformations',
        KeyConditionExpression: 'shop_id = :shop_id',
        ExpressionAttributeValues: {
          ':shop_id': retrievedShopId
        }
      })
    ),
    dynamoDB.send(
      new QueryCommand({
        TableName: 'Card_Design',
        KeyConditionExpression: 'shop_id = :shop_id',
        ExpressionAttributeValues: {
          ':shop_id': retrievedShopId
        }
      })
    ),
    dynamoDB.send(
      new QueryCommand({
        TableName: 'CustomerFacingDetails',
        KeyConditionExpression: 'shop_id = :shop_id',
        ExpressionAttributeValues: {
          ':shop_id': retrievedShopId
        }
      })
    ),
    dynamoDB.send(
      new QueryCommand({
        TableName: 'StampData',
        KeyConditionExpression: 'shop_id = :shop_id',
        ExpressionAttributeValues: {
          ':shop_id': retrievedShopId
        }
      })
    )
  ]);

  // Unmarshal DynamoDB format data from all 4 tables
  const unmarshalItems = (items) => {
    if (!items || !Array.isArray(items)) return [];
    return items.map(item => unmarshalDynamoDBItem(item));
  };

  // Store in ReleaseHistory for audit trail
  // Primary key: shop_id, Sort key: datetime (using sentdatetime value, ensures only 1 record per shop_id with this sentdatetime)
  await dynamoDB.send(
    new PutCommand({
      TableName: 'ReleaseHistory',
      Item: {
        shop_id: retrievedShopId,
        datetime: sentdatetime, // Sort key (datetime) - using sentdatetime value for sorting
        ...combinedData, // Include all combined data
        businessinformations: unmarshalItems(businessInfoResult.Items),
        Card_Design: unmarshalItems(cardDesignResult.Items),
        CustomerFacingDetails: unmarshalItems(customerFacingResult.Items),
        StampData: unmarshalItems(stampDataResult.Items)
      }
    })
  );
  
  // Update review_status to "accepted" and review_time timestamp in shop_release_contact
  await dynamoDB.send(
    new UpdateCommand({
      TableName: 'shop_release_contact',
      Key: {
        shop_id: shop_id
      },
      UpdateExpression: 'SET review_status = :status, review_time = :reviewTime',
      ExpressionAttributeValues: {
        ':status': 'accepted',
        ':reviewTime': combinedData.accepted_at
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
  
  // Final step: Check if Rejected_Customer_Review contains this shop and delete it if found
  console.log(`[${shop_id}] Final step: Checking and deleting from Rejected_Customer_Review...`);
  try {
    const rejectedResult = await dynamoDB.send(
      new QueryCommand({
        TableName: 'Rejected_Customer_Review',
        KeyConditionExpression: 'shop_id = :shop_id',
        ExpressionAttributeValues: {
          ':shop_id': shop_id
        }
      })
    );
    
    if (rejectedResult.Items && rejectedResult.Items.length > 0) {
      // Delete all rejected entries for this shop_id
      // Rejected_Customer_Review has shop_id as the primary key (no sort key)
      for (const rejectedItem of rejectedResult.Items) {
        // Only shop_id is needed for deletion (PK only, no sort key)
        const deleteKey = {
          shop_id: shop_id
        };
        
        await dynamoDB.send(
          new DeleteCommand({
            TableName: 'Rejected_Customer_Review',
            Key: deleteKey
          })
        );
      }
      console.log(`[${shop_id}] SUCCESS: Deleted ${rejectedResult.Items.length} entry/entries from Rejected_Customer_Review`);
    } else {
      console.log(`[${shop_id}] No entries found in Rejected_Customer_Review for this shop_id`);
    }
  } catch (error) {
    // Log error but don't fail the entire operation
    console.error(`[${shop_id}] FAILED: Error checking/deleting from Rejected_Customer_Review -`, error.message);
  }
  
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

// Get accepted customer reviews with location info (by shop_id from request body)
exports.getAcceptedReviewsWithAddress = asyncHandler(async (req, res, next) => {
  const { shop_id } = req.body;

  console.log(`[${shop_id}] Starting getAcceptedReviewsWithAddress process`);

  if (!shop_id) {
    console.error(`[${shop_id}] FAILED: shop_id is required in request body`);
    return res.status(400).json({
      success: false,
      error: 'shop_id is required in request body'
    });
  }

  // Get entries from Accepted_Customer_Review for the given shop_id
  let acceptedResult;
  try {
    console.log(`[${shop_id}] Step 1: Querying Accepted_Customer_Review...`);
    acceptedResult = await dynamoDB.send(
      new QueryCommand({
        TableName: 'Accepted_Customer_Review',
        KeyConditionExpression: 'shop_id = :shop_id',
        ExpressionAttributeValues: {
          ':shop_id': shop_id
        }
      })
    );

    if (!acceptedResult.Items || acceptedResult.Items.length === 0) {
      console.error(`[${shop_id}] FAILED: No records found in Accepted_Customer_Review`);
      return res.status(404).json({
        success: false,
        error: `No records found in Accepted_Customer_Review for shop_id ${shop_id}`
      });
    }
    console.log(`[${shop_id}] SUCCESS: Found ${acceptedResult.Items.length} record(s) in Accepted_Customer_Review`);
  } catch (error) {
    console.error(`[${shop_id}] FAILED: Error querying Accepted_Customer_Review -`, error.message);
    throw error;
  }

  const shopIds = [shop_id];
  let customerFacing, geocodedLocations;
  let liveBannerUrl = null, liveThumbnailUrl = null; // Store live bucket URLs for reuse

  // Query CustomerFacingDetails for this shop_id
  try {
    console.log(`[${shop_id}] Step 2: Querying CustomerFacingDetails...`);
    const cfResult = await dynamoDB.send(
      new QueryCommand({
        TableName: 'CustomerFacingDetails',
        KeyConditionExpression: 'shop_id = :shop_id',
        ExpressionAttributeValues: {
          ':shop_id': shop_id
        }
      })
    );

    if (!cfResult.Items || cfResult.Items.length === 0) {
      console.error(`[${shop_id}] FAILED: CustomerFacingDetails not found`);
      throw new Error(`CustomerFacingDetails not found for shop_id ${shop_id}`);
    }
    console.log(`[${shop_id}] SUCCESS: Found CustomerFacingDetails`);

    const cf = cfResult.Items[0];
    
    // Transform halalcertified: "yes" -> true, "no" -> false, "not applicable" or others -> null
    let halalcertified = null;
    if (cf.halalcertified) {
      const halalValue = String(cf.halalcertified).toLowerCase().trim();
      if (halalValue === 'yes') {
        halalcertified = true;
      } else if (halalValue === 'no') {
        halalcertified = false;
      }
      // "not applicable" or any other value becomes null
    }
    
    customerFacing = {
      keywords: cf.keywords ?? null,
      description: cf.description ?? null,
      displayName: cf.displayName ?? null,
      banner: cf.banner ?? null,
      thumbnail: cf.thumbnail ?? null,
      category: cf.category ?? null,
      halalcertified: halalcertified,
      website: cf.website ?? null // Optional field - not compulsory
    };

    // Copy banner and thumbnail to live bucket once at the start
    console.log(`[${shop_id}] Copying banner and thumbnail to live bucket...`);
    if (customerFacing.banner) {
      liveBannerUrl = await moveImageToLiveBucket(customerFacing.banner);
      console.log(`[${shop_id}] Banner copied: ${liveBannerUrl || 'failed'}`);
    }
    if (customerFacing.thumbnail) {
      liveThumbnailUrl = await moveImageToLiveBucket(customerFacing.thumbnail);
      console.log(`[${shop_id}] Thumbnail copied: ${liveThumbnailUrl || 'failed'}`);
    }

    const locations = Array.isArray(cf.locations) ? cf.locations : [];
    if (locations.length === 0) {
      console.error(`[${shop_id}] FAILED: Locations not found in CustomerFacingDetails`);
      throw new Error(`Locations not found for shop_id ${shop_id}`);
    }
    console.log(`[${shop_id}] SUCCESS: Found ${locations.length} location(s) to geocode`);

    // Geocode each location sequentially (respect rate limits)
    geocodedLocations = [];
    for (let locIndex = 0; locIndex < locations.length; locIndex++) {
      const loc = locations[locIndex];
      const postalCode = loc.postalCode ?? loc.postalcode ?? null;
      if (!postalCode || typeof postalCode !== 'string' || postalCode.trim() === '') {
        console.error(`[${shop_id}] FAILED: Missing postalCode at location index ${locIndex}`);
        throw new Error(`Missing postalCode for shop_id ${shop_id} at location index ${locIndex}`);
      }

      try {
        console.log(`[${shop_id}] Step 3.${locIndex + 1}: Geocoding location ${locIndex + 1}/${locations.length} (postalCode: ${postalCode})...`);
        const coordinates = await geocodeAddress(null, null, null, postalCode);
        geocodedLocations.push({
          id: loc.id ?? loc.locationId ?? null,
          locationName: loc.locationName ?? null,
          addressId: loc.addressId ?? loc.addressID ?? null,
          address: loc.address ?? loc.addressLine ?? null,
          postalCode: postalCode,
          latitude: coordinates.latitude,
          longitude: coordinates.longitude
        });
        console.log(`[${shop_id}] SUCCESS: Geocoded location ${locIndex + 1} (lat: ${coordinates.latitude}, lon: ${coordinates.longitude})`);

        if (locIndex < locations.length - 1) {
          await new Promise(resolve => setTimeout(resolve, 1000));
        }
      } catch (error) {
        console.error(`[${shop_id}] FAILED: Geocoding failed for location ${locIndex + 1} (postalCode: ${postalCode}) -`, error.message);
        throw error;
      }
    }
    console.log(`[${shop_id}] SUCCESS: All ${geocodedLocations.length} location(s) geocoded successfully`);
  } catch (error) {
    console.error(`[${shop_id}] FAILED: Error in CustomerFacingDetails/geocoding step -`, error.message);
    throw error;
  }

  // Combine accepted review data with customer-facing and locations
  const resultData = acceptedResult.Items.map(item => ({
    ...item,
    customerFacing: customerFacing,
    locations: geocodedLocations
  }));
  
  // Push to Supabase (if configured)
  if (supabase) {
    try {
      console.log(`[${shop_id}] Step 4: Inserting data into Supabase...`);

      // Use already copied banner and thumbnail URLs (copied earlier)

      // Insert one row per location (location_id is the primary key in Supabase)
      const supabaseRows = [];
      resultData.forEach(item => {
        geocodedLocations.forEach((loc, idx) => {
          const locationId = loc.id;
          if (!locationId) {
            throw new Error(`Missing location id for shop_id ${item.shop_id} at location index ${idx}`);
          }
          supabaseRows.push({
            location_id: locationId,
            shop_id: item.shop_id,
            keywords: customerFacing.keywords,
            description: customerFacing.description,
            display_name: customerFacing.displayName,
            category: customerFacing.category,
            halalcertified: customerFacing.halalcertified,
            thumbnail: liveThumbnailUrl,
            location_name: loc.locationName ?? null,
            address: loc.address ?? null,
            postal_code: loc.postalCode ?? null,
            latitude: loc.latitude,
            longitude: loc.longitude,
            rating: 5,
            ranking: 10
          });
        });
      });

      const { error: supabaseError } = await supabase
        .from('accepted_reviews_with_address')
        .upsert(supabaseRows, { onConflict: 'location_id' });

      if (supabaseError) {
        console.error(`[${shop_id}] FAILED: Supabase insertion error -`, supabaseError.message);
        throw new Error(`Supabase insertion failed: ${supabaseError.message}`);
      }
      console.log(`[${shop_id}] SUCCESS: Data inserted into Supabase`);
    } catch (error) {
      console.error(`[${shop_id}] FAILED: Supabase insertion step -`, error.message);
      throw error;
    }

    // Only proceed if Supabase insertion was successful
    // Retrieve data from all 4 tables and store in live_shop_details
    try {
      console.log(`[${shop_id}] Step 5: Querying 4 tables (businessinformations, Card_Design, CustomerFacingDetails, StampData)...`);
      const [businessInfoResult, cardDesignResult, customerFacingResult, stampDataResult] = await Promise.all([
        dynamoDB.send(
          new QueryCommand({
            TableName: 'businessinformations',
            KeyConditionExpression: 'shop_id = :shop_id',
            ExpressionAttributeValues: {
              ':shop_id': shop_id
            }
          })
        ),
        dynamoDB.send(
          new QueryCommand({
            TableName: 'Card_Design',
            KeyConditionExpression: 'shop_id = :shop_id',
            ExpressionAttributeValues: {
              ':shop_id': shop_id
            }
          })
        ),
        dynamoDB.send(
          new QueryCommand({
            TableName: 'CustomerFacingDetails',
            KeyConditionExpression: 'shop_id = :shop_id',
            ExpressionAttributeValues: {
              ':shop_id': shop_id
            }
          })
        ),
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
      console.log(`[${shop_id}] SUCCESS: Queried all 4 tables (businessinformations: ${businessInfoResult.Items?.length || 0}, Card_Design: ${cardDesignResult.Items?.length || 0}, CustomerFacingDetails: ${customerFacingResult.Items?.length || 0}, StampData: ${stampDataResult.Items?.length || 0})`);

      // Unmarshal DynamoDB format data from all 4 tables
      const unmarshalItems = (items) => {
        if (!items || !Array.isArray(items)) return [];
        return items.map(item => unmarshalDynamoDBItem(item));
      };

      // Unmarshal CustomerFacingDetails and replace banner/thumbnail with live bucket URLs
      const unmarshaledCustomerFacing = unmarshalItems(customerFacingResult.Items);
      if (unmarshaledCustomerFacing.length > 0) {
        // Replace banner and thumbnail with live bucket URLs
        unmarshaledCustomerFacing.forEach(cfItem => {
          if (liveBannerUrl) {
            cfItem.banner = liveBannerUrl;
          }
          if (liveThumbnailUrl) {
            cfItem.thumbnail = liveThumbnailUrl;
          }
        });
      }

      // Combine all data from the 4 tables (unmarshaled, with updated banner/thumbnail URLs)
      const liveShopDetails = {
        shop_id: shop_id,
        businessinformations: unmarshalItems(businessInfoResult.Items),
        Card_Design: unmarshalItems(cardDesignResult.Items),
        CustomerFacingDetails: unmarshaledCustomerFacing,
        StampData: unmarshalItems(stampDataResult.Items),
        created_at: new Date().toISOString()
      };

      // Store in live_shop_details table in Supabase
      console.log(`[${shop_id}] Step 6: Storing data in live_shop_details table (Supabase)...`);
      const { error: liveShopError } = await supabase
        .from('live_shop_details')
        .upsert(liveShopDetails, { onConflict: 'shop_id' });

      if (liveShopError) {
        console.error(`[${shop_id}] FAILED: Supabase live_shop_details insertion error -`, liveShopError.message);
        throw new Error(`Supabase live_shop_details insertion failed: ${liveShopError.message}`);
      }
      console.log(`[${shop_id}] SUCCESS: Data stored in live_shop_details table (Supabase)`);

      // Delete entry from Accepted_Customer_Review after successful storage
      console.log(`[${shop_id}] Step 7: Deleting entry from Accepted_Customer_Review...`);
      const acceptedItem = acceptedResult.Items[0];
      
      // Get sentdatetime from accepted item for ReleaseHistory sort key
      const sentdatetime = acceptedItem.sentdatetime;
      
      if (!sentdatetime) {
        console.warn(`[${shop_id}] WARNING: sentdatetime not found in accepted item, skipping ReleaseHistory update`);
      } else {
        // Retrieve the stored live_shop_details from Supabase to get complete data
        // Then add it to DynamoDB ReleaseHistory
        try {
          console.log(`[${shop_id}] Step 6.5: Retrieving live_shop_details from Supabase and adding to ReleaseHistory (DynamoDB)...`);
          const { data: retrievedLiveShopDetails, error: retrieveError } = await supabase
            .from('live_shop_details')
            .select('*')
            .eq('shop_id', shop_id)
            .single();

          // Use retrieved data if available, otherwise fallback to local data
          const liveShopDetailsForHistory = retrieveError ? liveShopDetails : retrievedLiveShopDetails;
          
          if (retrieveError) {
            console.warn(`[${shop_id}] WARNING: Could not retrieve live_shop_details from Supabase, using local data -`, retrieveError.message);
          }

          // Add all live_shop_details data to ReleaseHistory
          // PK: shop_id, SK: datetime (using sentdatetime value, ensures only 1 record per shop_id with this sentdatetime)
          // Includes: businessinformations, Card_Design, CustomerFacingDetails, StampData (from liveShopDetailsForHistory)
          const releaseHistoryItem = {
            shop_id: shop_id,
            datetime: sentdatetime, // Sort key (datetime) - using sentdatetime value for sorting
            ...liveShopDetailsForHistory, // Include all live_shop_details data from Supabase (includes the 4 tables)
            source: 'live_shop_details' // Mark that this came from live_shop_details
          };

          await dynamoDB.send(
            new PutCommand({
              TableName: 'ReleaseHistory',
              Item: releaseHistoryItem
            })
          );
          console.log(`[${shop_id}] SUCCESS: live_shop_details added to ReleaseHistory (DynamoDB) with sentdatetime: ${sentdatetime}`);
        } catch (error) {
          // Log error but don't fail the entire operation
          console.error(`[${shop_id}] FAILED: Error adding live_shop_details to ReleaseHistory -`, error.message);
        }
      }
      
      // Since the query only uses shop_id in KeyConditionExpression, 
      // the table likely only has shop_id as the partition key (no sort key)
      // If it had a sort key, we would need to include it in the delete operation
      const deleteKey = {
        shop_id: shop_id
      };

      await dynamoDB.send(
        new DeleteCommand({
          TableName: 'Accepted_Customer_Review',
          Key: deleteKey
        })
      );
      console.log(`[${shop_id}] SUCCESS: Entry deleted from Accepted_Customer_Review`);
    } catch (error) {
      console.error(`[${shop_id}] FAILED: Error in live_shop_details storage/deletion step -`, error.message);
      throw error;
    }
  } else {
    console.warn(`[${shop_id}] WARNING: Supabase not configured; skipping Supabase upsert and live_shop_details storage`);
  }

  console.log(`[${shop_id}] SUCCESS: Process completed successfully`);
  res.status(200).json({
    success: true,
    count: resultData.length,
    data: resultData
  });
});

