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
} from "../../shared/logger";

const dynamoClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const docClient = DynamoDBDocumentClient.from(dynamoClient);

const PRODUCT_TABLE_NAME = process.env.PRODUCT_COLLECTION_TABLE!;

enum StockOperationType {
  DECREASE = "DECREASE",
  INCREASE = "INCREASE",
}

interface StockUpdateMessage {
  productId: string;
  quantity: number;
  operation: StockOperationType;
  orderId?: string;
  reason?: string;
}

interface Product {
  productId: string;
  name: string;
  price: number;
  category: string;
  description?: string;
  isActive: boolean;
  quantityInStock?: number;
}

export const handler = async (event: SQSEvent): Promise<void> => {
  const processId = generateCorrelationId("stock-update");
  const logger = createLogger({
    processId,
    functionName: "update-product-stock",
  });

  const mainTracker = new PerformanceTracker(logger, "stock-update-batch");

  logger.info("Processing stock update batch started", {
    recordsCount: event.Records.length,
  });

  for (const [index, record] of event.Records.entries()) {
    const recordLogger = logger.withContext({
      recordIndex: index,
      messageId: record.messageId,
    });

    const recordTracker = new PerformanceTracker(
      recordLogger,
      "stock-update-record"
    );

    recordLogger.info("Processing SQS record", {
      receiptHandle: record.receiptHandle.substring(0, 50) + "...",
      body: record.body.substring(0, 200) + "...",
    });

    try {
      const message: StockUpdateMessage = JSON.parse(record.body);

      const stockLogger = recordLogger.withContext({
        productId: message.productId,
        operation: message.operation,
        quantity: message.quantity,
      });

      stockLogger.info("Stock update message parsed successfully", {
        productId: message.productId,
        operation: message.operation,
        quantity: message.quantity,
        orderId: message.orderId,
        reason: message.reason,
      });

      const processingTracker = new PerformanceTracker(
        stockLogger,
        "stock-update-logic"
      );

      stockLogger.info("Starting stock update logic");

      await updateProductStock(message, stockLogger);

      processingTracker.finish({
        productId: message.productId,
        operation: message.operation,
        quantity: message.quantity,
      });

      stockLogger.info("Stock update completed successfully", {
        productId: message.productId,
        operation: message.operation,
        quantity: message.quantity,
        orderId: message.orderId,
      });

      recordTracker.finish({
        productId: message.productId,
        status: "success",
      });
    } catch (error) {
      recordLogger.error("Error processing stock update record", error, {
        sqsMessagePreview: record.body.substring(0, 200),
      });

      recordTracker.finishWithError(error);

      throw error;
    }
  }

  mainTracker.finish({
    totalRecords: event.Records.length,
    status: "completed",
  });

  logger.info("Stock update batch processing completed", {
    totalRecords: event.Records.length,
  });
};

async function updateProductStock(
  message: StockUpdateMessage,
  logger: any
): Promise<void> {
  const updateTracker = new PerformanceTracker(logger, "product-stock-update");

  try {
    logger.info("Starting product stock update", {
      productId: message.productId,
      operation: message.operation,
      quantity: message.quantity,
    });

    const product = await getProductById(message.productId, logger);

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
      });
      throw error;
    }

    let updateExpression: string;
    let conditionExpression: string;
    const expressionAttributeValues: any = {
      ":quantity": message.quantity,
      ":updatedAt": new Date().toISOString(),
    };

    if (message.operation === StockOperationType.DECREASE) {
      updateExpression =
        "SET quantityInStock = quantityInStock - :quantity, updatedAt = :updatedAt";
      conditionExpression =
        "quantityInStock >= :quantity AND isActive = :isActive";
      expressionAttributeValues[":isActive"] = true;

      logger.info("Preparing stock reduction", {
        productId: message.productId,
        productName: product.name,
        currentStock: product.quantityInStock,
        quantityToReduce: message.quantity,
        finalStock: product.quantityInStock - message.quantity,
      });
    } else if (message.operation === StockOperationType.INCREASE) {
      updateExpression =
        "SET quantityInStock = quantityInStock + :quantity, updatedAt = :updatedAt";
      conditionExpression = "isActive = :isActive";
      expressionAttributeValues[":isActive"] = true;

      logger.info("Preparing stock addition", {
        productId: message.productId,
        productName: product.name,
        currentStock: product.quantityInStock,
        quantityToAdd: message.quantity,
        finalStock: product.quantityInStock + message.quantity,
      });
    } else {
      const error = new Error(
        `Invalid operation type: ${message.operation}. Must be DECREASE or INCREASE`
      );
      logger.error("Invalid stock operation type", error, {
        productId: message.productId,
        operation: message.operation,
      });
      throw error;
    }

    const updateCommand = new UpdateCommand({
      TableName: PRODUCT_TABLE_NAME,
      Key: {
        productId: message.productId,
      },
      UpdateExpression: updateExpression,
      ConditionExpression: conditionExpression,
      ExpressionAttributeValues: expressionAttributeValues,
      ReturnValues: "ALL_NEW",
    });

    const result = await docClient.send(updateCommand);

    updateTracker.finish({
      productId: message.productId,
      operation: message.operation,
      quantity: message.quantity,
      previousStock: product.quantityInStock,
      newStock: result.Attributes?.quantityInStock as number,
    });

    logger.info("Stock updated successfully", {
      productId: message.productId,
      productName: product.name,
      operation: message.operation,
      previousStock: product.quantityInStock,
      newStock: result.Attributes?.quantityInStock as number,
      quantityChanged: message.quantity,
      orderId: message.orderId,
      reason: message.reason,
    });
  } catch (error: any) {
    logger.error("Failed to update product stock", error, {
      productId: message.productId,
      operation: message.operation,
      quantity: message.quantity,
      errorName: error.name,
      orderId: message.orderId,
    });

    updateTracker.finishWithError(error);

    if (error.name === "ConditionalCheckFailedException") {
      if (message.operation === StockOperationType.DECREASE) {
        throw new Error(
          `Insufficient stock for product ${message.productId}. Cannot reduce stock by ${message.quantity}.`
        );
      } else {
        throw new Error(
          `Cannot update stock for product ${message.productId}. Product may be inactive.`
        );
      }
    }

    throw error;
  }
}

async function getProductById(
  productId: string,
  logger: any
): Promise<Product | null> {
  const productTracker = new PerformanceTracker(logger, "product-lookup");

  try {
    logger.info("Looking up product for stock update", {
      productId: productId,
    });

    const getCommand = new GetCommand({
      TableName: PRODUCT_TABLE_NAME,
      Key: {
        productId: productId,
      },
      ProjectionExpression:
        "productId, #name, price, category, description, isActive, quantityInStock",
      ExpressionAttributeNames: {
        "#name": "name",
      },
    });

    const result = await docClient.send(getCommand);

    if (result.Item) {
      const product: Product = {
        productId: result.Item.productId as string,
        name: result.Item.name as string,
        price: result.Item.price as number,
        category: result.Item.category as string,
        description: result.Item.description as string,
        isActive: result.Item.isActive as boolean,
        quantityInStock: result.Item.quantityInStock as number | undefined,
      };

      logger.info("Product found for stock update", {
        productId: productId,
        productName: product.name,
        isActive: product.isActive,
        hasStockControl: product.quantityInStock !== undefined,
        currentStock: product.quantityInStock,
      });

      productTracker.finish({
        productId: productId,
        found: true,
        hasStockControl: product.quantityInStock !== undefined,
      });

      return product;
    } else {
      logger.warn("Product not found for stock update", {
        productId: productId,
      });

      productTracker.finish({
        productId: productId,
        found: false,
      });

      return null;
    }
  } catch (error) {
    logger.error("Error looking up product for stock update", error, {
      productId: productId,
    });

    productTracker.finishWithError(error);
    throw error;
  }
}
