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
import { OrderStatus, OrderData } from "../../shared/schemas/order";

const dynamoClient = new DynamoDBClient({ 
  region: process.env.AWS_REGION || 'us-east-1' 
});
const docClient = DynamoDBDocumentClient.from(dynamoClient);

const ORDER_TABLE_NAME = process.env.ORDER_COLLECTION_TABLE!;

export interface UpdateOrderMessage {
  orderId: string;
  status: OrderStatus;
  reason?: string;
  transactionId?: string;
}

export const handler = async (event: SQSEvent): Promise<void> => {
  const processId = generateCorrelationId("update-order");
  const logger = createLogger({
    processId,
    functionName: "update-order",
  });

  const mainTracker = new PerformanceTracker(logger, "update-order-batch");

  logger.info("Processing update order batch started", {
    recordsCount: event.Records.length,
  });

  for (const [index, record] of event.Records.entries()) {
    const recordLogger = logger.withContext({
      recordIndex: index,
      messageId: record.messageId,
    });

    const recordTracker = new PerformanceTracker(
      recordLogger,
      "update-order-record"
    );

    recordLogger.info("Processing SQS record", {
      receiptHandle: record.receiptHandle.substring(0, 50) + "...",
      body: record.body.substring(0, 200) + "...",
    });

    try {
      const message: UpdateOrderMessage = JSON.parse(record.body);

      const orderLogger = recordLogger.withContext({
        orderId: message.orderId,
      });

      orderLogger.info("Update order message parsed successfully", {
        orderId: message.orderId,
        newStatus: message.status,
        reason: message.reason,
        transactionId: message.transactionId,
      });

      const processingTracker = new PerformanceTracker(
        orderLogger,
        "order-update-logic"
      );

      orderLogger.info("Starting order update processing");

      await updateOrderRecord(message, orderLogger);

      processingTracker.finish({
        orderId: message.orderId,
        newStatus: message.status,
      });

      orderLogger.info("Order update processing completed successfully", {
        orderId: message.orderId,
        newStatus: message.status,
        transactionId: message.transactionId,
      });

      recordTracker.finish({
        orderId: message.orderId,
        status: "success",
      });
    } catch (error) {
      recordLogger.error("Error processing update order record", error, {
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

  logger.info("Update order batch processing completed", {
    totalRecords: event.Records.length,
  });
};

async function updateOrderRecord(
  message: UpdateOrderMessage,
  logger: any
): Promise<void> {
  const updateTracker = new PerformanceTracker(
    logger,
    "order-record-update"
  );

  try {
    logger.info("Starting order record update", {
      orderId: message.orderId,
      newStatus: message.status,
      reason: message.reason,
      transactionId: message.transactionId,
    });

    const order = await getOrderById(message.orderId, logger);

    if (!order) {
      const error = new Error(`Order not found: ${message.orderId}`);
      logger.error("Order not found for update", error, {
        orderId: message.orderId,
      });
      throw error;
    }

    logger.info("Order found for update", {
      orderId: message.orderId,
      currentStatus: order.status,
      newStatus: message.status,
      totalValue: order.totalValue,
      leadId: order.leadId,
    });

    if (!isValidStatusTransition(order.status, message.status)) {
      const error = new Error(
        `Invalid status transition from ${order.status} to ${message.status}`
      );
      logger.error("Invalid status transition", error, {
        orderId: message.orderId,
        currentStatus: order.status,
        newStatus: message.status,
      });
      throw error;
    }

    await updateOrderStatus(message, logger);

    updateTracker.finish({
      orderId: message.orderId,
      previousStatus: order.status,
      newStatus: message.status,
    });

    logger.info("Order record updated successfully", {
      orderId: message.orderId,
      previousStatus: order.status,
      newStatus: message.status,
      reason: message.reason,
      transactionId: message.transactionId,
    });

  } catch (error: any) {
    logger.error("Failed to update order record", error, {
      orderId: message.orderId,
      newStatus: message.status,
      errorName: error.name,
    });

    updateTracker.finishWithError(error);
    throw error;
  }
}

async function getOrderById(
  orderId: string,
  logger: any
): Promise<OrderData | null> {
  const orderTracker = new PerformanceTracker(
    logger,
    "order-lookup"
  );

  try {
    logger.info("Looking up order for update", {
      orderId: orderId,
    });

    const getCommand = new GetCommand({
      TableName: ORDER_TABLE_NAME,
      Key: {
        orderId: orderId,
      },
    });

    const result = await docClient.send(getCommand);

    if (result.Item) {
      const order: OrderData = result.Item as OrderData;

      logger.info("Order found for update", {
        orderId: orderId,
        status: order.status,
        totalValue: order.totalValue,
        leadId: order.leadId,
        totalItems: order.totalItems,
      });

      orderTracker.finish({
        orderId: orderId,
        found: true,
        status: order.status,
      });

      return order;
    } else {
      logger.warn("Order not found for update", {
        orderId: orderId,
      });

      orderTracker.finish({
        orderId: orderId,
        found: false,
      });

      return null;
    }
  } catch (error) {
    logger.error("Error looking up order for update", error, {
      orderId: orderId,
    });

    orderTracker.finishWithError(error);
    throw error;
  }
}

async function updateOrderStatus(
  message: UpdateOrderMessage,
  logger: any
): Promise<void> {
  const updateTracker = new PerformanceTracker(
    logger,
    "order-status-update"
  );

  try {
    logger.info("Updating order status", {
      orderId: message.orderId,
      newStatus: message.status,
      reason: message.reason,
      transactionId: message.transactionId,
    });

    let updateExpression = "SET #status = :status, updatedAt = :updatedAt";
    const expressionAttributeNames: any = {
      "#status": "status",
    };
    const expressionAttributeValues: any = {
      ":status": message.status,
      ":updatedAt": new Date().toISOString(),
    };

    if (message.reason) {
      updateExpression += ", #reason = :reason";
      expressionAttributeNames["#reason"] = "reason";
      expressionAttributeValues[":reason"] = message.reason;
    }

    if (message.transactionId) {
      updateExpression += ", transactionId = :transactionId";
      expressionAttributeValues[":transactionId"] = message.transactionId;
    }

    const updateCommand = new UpdateCommand({
      TableName: ORDER_TABLE_NAME,
      Key: {
        orderId: message.orderId,
      },
      UpdateExpression: updateExpression,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: expressionAttributeValues,
      ReturnValues: "ALL_NEW",
    });

    await docClient.send(updateCommand);

    updateTracker.finish({
      orderId: message.orderId,
      newStatus: message.status,
    });

    logger.info("Order status updated successfully", {
      orderId: message.orderId,
      newStatus: message.status,
      reason: message.reason,
      transactionId: message.transactionId,
      updatedAt: expressionAttributeValues[":updatedAt"],
    });

  } catch (error: any) {
    logger.error("Failed to update order status", error, {
      orderId: message.orderId,
      newStatus: message.status,
      errorName: error.name,
    });

    updateTracker.finishWithError(error);
    throw error;
  }
}

function isValidStatusTransition(
  currentStatus: OrderStatus,
  newStatus: OrderStatus
): boolean {
  const validTransitions: Record<OrderStatus, OrderStatus[]> = {
    [OrderStatus.PENDING]: [
      OrderStatus.PROCESSED,
      OrderStatus.CANCELLED,
    ],
    [OrderStatus.PROCESSED]: [],
    [OrderStatus.CANCELLED]: [],
  };

  const allowedTransitions = validTransitions[currentStatus] || [];
  return allowedTransitions.includes(newStatus);
}
