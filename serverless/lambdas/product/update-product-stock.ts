import { SQSEvent } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  QueryCommand,
} from "@aws-sdk/lib-dynamodb";
import {
  createLogger,
  generateCorrelationId,
  PerformanceTracker,
  StructuredLogger,
} from "../../shared/logger";
import { ProductData } from "../../shared/schemas/product";
import { StockOperationType, ProductStockData } from "../../shared/schemas/product-stock";
import { v4 as uuidv4 } from "uuid";

const dynamoClient = new DynamoDBClient({ 
  region: process.env.AWS_REGION || 'us-east-1' 
});
const docClient = DynamoDBDocumentClient.from(dynamoClient);

const PRODUCT_TABLE_NAME = process.env.PRODUCT_COLLECTION_TABLE!;
const PRODUCT_STOCK_TABLE_NAME = "product-stock";

export interface StockUpdateMessage {
  productId: string;
  quantity: number;
  operation: StockOperationType;
  orderId: string;
  reason: string;
}

interface StockUpdateResult {
  productId: string;
  operation: StockOperationType;
  quantity: number;
  stockEntryId: string;
}

async function calculateCurrentStock(productId: string): Promise<number> {
  const queryParams = {
    TableName: PRODUCT_STOCK_TABLE_NAME,
    IndexName: "ProductStocksByProductId",
    KeyConditionExpression: "productId = :productId",
    ExpressionAttributeValues: {
      ":productId": productId,
    },
  };

  const result = await docClient.send(new QueryCommand(queryParams));
  
  if (!result.Items || result.Items.length === 0) {
    return 0;
  }

  let totalIncrease = 0;
  let totalDecrease = 0;

  for (const item of result.Items) {
    const entry = item as ProductStockData;
    if (entry.type === StockOperationType.INCREASE) {
      totalIncrease += entry.quantity;
    } else if (entry.type === StockOperationType.DECREASE) {
      totalDecrease += entry.quantity;
    }
  }

  return totalIncrease - totalDecrease;
}

async function createStockEntry(request: {
  productId: string;
  type: StockOperationType;
  quantity: number;
  reason: string;
  orderId?: string;
}): Promise<string> {
  const stockEntry: ProductStockData = {
    id: uuidv4(),
    productId: request.productId,
    type: request.type,
    quantity: request.quantity,
    reason: request.reason,
    orderId: request.orderId,
    createdAt: new Date().toISOString(),
  };

  await docClient.send(new PutCommand({
    TableName: PRODUCT_STOCK_TABLE_NAME,
    Item: stockEntry,
  }));

  return stockEntry.id;
}

function validateStockUpdateMessage(message: any): StockUpdateMessage {
  const requiredFields = ['productId', 'quantity', 'operation', 'orderId', 'reason'];
  const missingFields = requiredFields.filter(field => !message[field]);
  
  if (missingFields.length > 0) {
    throw new Error(`Missing required fields: ${missingFields.join(', ')}`);
  }

  if (!Object.values(StockOperationType).includes(message.operation)) {
    throw new Error(`Invalid operation type: ${message.operation}. Must be DECREASE or INCREASE`);
  }

  if (typeof message.quantity !== 'number' || message.quantity <= 0) {
    throw new Error(`Invalid quantity: ${message.quantity}. Must be a positive number`);
  }

  return message as StockUpdateMessage;
}

export const handler = async (event: SQSEvent): Promise<void> => {
  const processId = generateCorrelationId("stock-update");
  const logger = createLogger({
    processId,
    functionName: "update-product-stock",
  });

  const batchTracker = new PerformanceTracker(logger, "stock-update-batch");

  logger.info("Processing stock update batch started", {
    recordsCount: event.Records.length,
  });

  for (let i = 0; i < event.Records.length; i++) {
    const record = event.Records[i];
    const recordLogger = logger.withContext({
      recordIndex: i,
      messageId: record.messageId,
    });

    const recordTracker = new PerformanceTracker(
      recordLogger,
      "stock-update-record"
    );

    try {
      recordLogger.info("Processing stock update record", {
        receiptHandle: record.receiptHandle?.substring(0, 30) + "...",
        bodyPreview: record.body.substring(0, 100) + "...",
      });

      const rawMessage = JSON.parse(record.body);
      const stockUpdateMessage = validateStockUpdateMessage(rawMessage);

      const result = await processStockUpdate(stockUpdateMessage, recordLogger);

      recordTracker.finish({
        productId: stockUpdateMessage.productId,
        operation: stockUpdateMessage.operation,
        quantity: stockUpdateMessage.quantity,
        status: "success",
      });

      recordLogger.info("Stock update record processed successfully", {
        productId: result.productId,
        operation: result.operation,
        quantity: result.quantity,
        stockEntryId: result.stockEntryId,
      });

    } catch (error: any) {
      recordLogger.error("Error processing stock update record", error, {
        sqsMessagePreview: record.body.substring(0, 200),
        errorType: error.constructor.name,
        recordIndex: i,
      });

      recordTracker.finishWithError(error);
    }
  }

  batchTracker.finish({
    totalRecords: event.Records.length,
    status: "completed",
  });

  logger.info("Stock update batch processing completed", {
    totalRecords: event.Records.length,
  });
};

async function processStockUpdate(
  message: StockUpdateMessage,
  logger: StructuredLogger
): Promise<StockUpdateResult> {
  const updateLogger = logger.withContext({
    productId: message.productId,
    operation: message.operation,
    quantity: message.quantity,
    orderId: message.orderId,
  });

  const processingTracker = new PerformanceTracker(updateLogger, "stock-update-processing");

  updateLogger.info("Starting stock update processing", {
    productId: message.productId,
    operation: message.operation,
    quantity: message.quantity,
    orderId: message.orderId,
    reason: message.reason,
  });

  try {
    const product = await getProductById(message.productId, updateLogger);
    
    validateProductForStockUpdate(product, message, updateLogger);
    
    if (message.operation === StockOperationType.DECREASE) {
      const currentStock = await calculateCurrentStock(message.productId);
      
      if (currentStock < message.quantity) {
        const error = new Error(
          `Insufficient stock for product ${message.productId}. Current: ${currentStock}, Required: ${message.quantity}`
        );
        updateLogger.error("Insufficient stock for operation", error, {
          productId: message.productId,
          currentStock,
          requiredQuantity: message.quantity,
        });
        throw error;
      }
    }

    const stockEntryId = await createStockEntry({
      productId: message.productId,
      type: message.operation,
      quantity: message.quantity,
      reason: message.reason,
      orderId: message.orderId,
    });

    const result: StockUpdateResult = {
      productId: message.productId,
      operation: message.operation,
      quantity: message.quantity,
      stockEntryId,
    };

    processingTracker.finish({
      productId: message.productId,
      operation: message.operation,
      quantity: message.quantity,
      stockEntryId,
    });

    updateLogger.info("Stock update processing completed successfully", {
      productId: result.productId,
      operation: result.operation,
      quantityChanged: result.quantity,
      stockEntryId: result.stockEntryId,
    });

    return result;

  } catch (error: any) {
    updateLogger.error("Stock update processing failed", error, {
      productId: message.productId,
      operation: message.operation,
      quantity: message.quantity,
      orderId: message.orderId,
    });

    processingTracker.finishWithError(error);
    throw error;
  }
}

function validateProductForStockUpdate(
  product: ProductData | null,
  message: StockUpdateMessage,
  logger: StructuredLogger
): void {
  if (!product) {
    const error = new Error(`Product not found: ${message.productId}`);
    logger.error("Product not found for stock update", error, {
      productId: message.productId,
    });
    throw error;
  }

  if (!product.isActive) {
    const error = new Error(`Product is inactive: ${message.productId}`);
    logger.error("Cannot update stock for inactive product", error, {
      productId: message.productId,
      productName: product.name,
      isActive: product.isActive,
    });
    throw error;
  }

  logger.info("Product validation successful", {
    productId: message.productId,
    productName: product.name,
    isActive: product.isActive,
  });
}

async function getProductById(
  productId: string,
  logger: StructuredLogger
): Promise<ProductData | null> {
  const lookupTracker = new PerformanceTracker(logger, "product-lookup");

  try {
    logger.info("Looking up product for stock update", {
      productId,
    });

    const getCommand = new GetCommand({
      TableName: PRODUCT_TABLE_NAME,
      Key: {
        id: productId,
      },
      ProjectionExpression:
        "id, #name, price, category, description, isActive",
      ExpressionAttributeNames: {
        "#name": "name",
      },
    });

    const result = await docClient.send(getCommand);

    if (result.Item) {
      const product = mapDynamoItemToProduct(result.Item);

      lookupTracker.finish({
        productId,
        found: true,
        isActive: product.isActive,
      });

      logger.info("Product found for stock update", {
        productId,
        productName: product.name,
        isActive: product.isActive,
      });

      return product;
    } else {
      lookupTracker.finish({
        productId,
        found: false,
      });

      logger.warn("Product not found for stock update", {
        productId,
      });

      return null;
    }
  } catch (error: any) {
    logger.error("Error looking up product for stock update", error, {
      productId,
      errorType: error.constructor.name,
    });

    lookupTracker.finishWithError(error);
    throw error;
  }
}

function mapDynamoItemToProduct(item: any): ProductData {
  return {
    id: item.id as string,
    name: item.name as string,
    price: item.price as number,
    description: item.description as string,
    isActive: item.isActive as boolean,
  };
}
