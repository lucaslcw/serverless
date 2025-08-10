import { SQSEvent } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  GetCommand,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";
import {
  createLogger,
  generateCorrelationId,
  PerformanceTracker,
  StructuredLogger,
} from "../../shared/logger";
import { ProductData } from "../../shared/schemas/product";

const dynamoClient = new DynamoDBClient({ 
  region: process.env.AWS_REGION || 'us-east-1' 
});
const docClient = DynamoDBDocumentClient.from(dynamoClient);

const PRODUCT_TABLE_NAME = process.env.PRODUCT_COLLECTION_TABLE!;

export enum StockOperationType {
  DECREASE = "DECREASE",
  INCREASE = "INCREASE",
}

export interface StockUpdateMessage {
  productId: string;
  quantity: number;
  operation: StockOperationType;
  orderId: string;
  reason: string;
}

interface StockUpdateResult {
  productId: string;
  previousStock: number;
  newStock: number;
  operation: StockOperationType;
  quantity: number;
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
        previousStock: result.previousStock,
        newStock: result.newStock,
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
    
    const result = await executeStockUpdate(product!, message, updateLogger);

    processingTracker.finish({
      productId: message.productId,
      operation: message.operation,
      quantity: message.quantity,
      previousStock: result.previousStock,
      newStock: result.newStock,
    });

    updateLogger.info("Stock update processing completed successfully", {
      productId: result.productId,
      operation: result.operation,
      previousStock: result.previousStock,
      newStock: result.newStock,
      quantityChanged: result.quantity,
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

  if (product.quantityInStock === undefined) {
    const error = new Error(
      `Product ${product.name} does not have stock control (quantityInStock property missing)`
    );
    logger.error("Product does not have stock control", error, {
      productId: message.productId,
      productName: product.name,
      hasStockControl: false,
    });
    throw error;
  }

  logger.info("Product validation successful", {
    productId: message.productId,
    productName: product.name,
    isActive: product.isActive,
    currentStock: product.quantityInStock,
    hasStockControl: true,
  });
}

async function executeStockUpdate(
  product: ProductData,
  message: StockUpdateMessage,
  logger: StructuredLogger
): Promise<StockUpdateResult> {
  const updateTracker = new PerformanceTracker(logger, "dynamo-stock-update");

  try {
    const { updateExpression, conditionExpression, expressionAttributeValues } = 
      buildUpdateParameters(product, message, logger);

    const updateCommand = new UpdateCommand({
      TableName: PRODUCT_TABLE_NAME,
      Key: {
        id: message.productId,
      },
      UpdateExpression: updateExpression,
      ConditionExpression: conditionExpression,
      ExpressionAttributeValues: expressionAttributeValues,
      ReturnValues: "ALL_NEW",
    });

    logger.info("Executing stock update in DynamoDB", {
      productId: message.productId,
      operation: message.operation,
      quantity: message.quantity,
      currentStock: product.quantityInStock,
    });

    const result = await docClient.send(updateCommand);
    const newStock = result.Attributes?.quantityInStock as number;

    const stockUpdateResult: StockUpdateResult = {
      productId: message.productId,
      previousStock: product.quantityInStock!,
      newStock,
      operation: message.operation,
      quantity: message.quantity,
    };

    updateTracker.finish({
      productId: message.productId,
      operation: message.operation,
      quantity: message.quantity,
      previousStock: product.quantityInStock,
      newStock,
    });

    logger.info("Stock updated successfully in DynamoDB", {
      productId: message.productId,
      productName: product.name,
      operation: message.operation,
      previousStock: product.quantityInStock,
      newStock,
      quantityChanged: message.quantity,
      orderId: message.orderId,
      reason: message.reason,
    });

    return stockUpdateResult;

  } catch (error: any) {
    logger.error("Failed to update product stock in DynamoDB", error, {
      productId: message.productId,
      operation: message.operation,
      quantity: message.quantity,
      errorName: error.name,
      orderId: message.orderId,
    });

    updateTracker.finishWithError(error);

    if (error.name === "ConditionalCheckFailedException") {
      throw createConditionalCheckError(message);
    }

    throw error;
  }
}

function buildUpdateParameters(
  product: ProductData,
  message: StockUpdateMessage,
  logger: StructuredLogger
): {
  updateExpression: string;
  conditionExpression: string;
  expressionAttributeValues: Record<string, any>;
} {
  const expressionAttributeValues: Record<string, any> = {
    ":quantity": message.quantity,
    ":updatedAt": new Date().toISOString(),
    ":isActive": true,
  };

  let updateExpression: string;
  let conditionExpression: string;

  if (message.operation === StockOperationType.DECREASE) {
    updateExpression =
      "SET quantityInStock = quantityInStock - :quantity, updatedAt = :updatedAt";
    conditionExpression =
      "quantityInStock >= :quantity AND isActive = :isActive";

    logger.info("Preparing stock reduction", {
      productId: message.productId,
      productName: product.name,
      currentStock: product.quantityInStock,
      quantityToReduce: message.quantity,
      projectedStock: product.quantityInStock! - message.quantity,
    });

  } else if (message.operation === StockOperationType.INCREASE) {
    updateExpression =
      "SET quantityInStock = quantityInStock + :quantity, updatedAt = :updatedAt";
    conditionExpression = "isActive = :isActive";

    logger.info("Preparing stock addition", {
      productId: message.productId,
      productName: product.name,
      currentStock: product.quantityInStock,
      quantityToAdd: message.quantity,
      projectedStock: product.quantityInStock! + message.quantity,
    });

  } else {
    throw new Error(
      `Invalid operation type: ${message.operation}. Must be DECREASE or INCREASE`
    );
  }

  return {
    updateExpression,
    conditionExpression,
    expressionAttributeValues,
  };
}

function createConditionalCheckError(message: StockUpdateMessage): Error {
  if (message.operation === StockOperationType.DECREASE) {
    return new Error(
      `Insufficient stock for product ${message.productId}. Cannot reduce stock by ${message.quantity}.`
    );
  } else {
    return new Error(
      `Cannot update stock for product ${message.productId}. Product may be inactive.`
    );
  }
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
        "id, #name, price, category, description, isActive, quantityInStock",
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
        hasStockControl: product.quantityInStock !== undefined,
        isActive: product.isActive,
      });

      logger.info("Product found for stock update", {
        productId,
        productName: product.name,
        isActive: product.isActive,
        hasStockControl: product.quantityInStock !== undefined,
        currentStock: product.quantityInStock,
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
    quantityInStock: item.quantityInStock as number | undefined,
  };
}
