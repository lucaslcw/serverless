import { APIGatewayProxyEvent, APIGatewayProxyResult } from "aws-lambda";
import { SNS } from "aws-sdk";
import {
  validateCreateOrderData,
  sanitizeCreateOrderData,
  createErrorResponse,
  createSuccessResponse,
} from "../../shared/validators";
import {
  createLogger,
  generateCorrelationId,
  maskSensitiveData,
  PerformanceTracker,
} from "../../shared/logger";

const sns = new SNS();

export const handler = async (
  event: APIGatewayProxyEvent
): Promise<APIGatewayProxyResult> => {
  const correlationId = generateCorrelationId();
  const logger = createLogger({
    correlationId,
    functionName: "initialize-order",
    httpMethod: event.httpMethod,
    path: event.path,
  });

  const tracker = new PerformanceTracker(logger, "initialize-order-request");

  try {
    logger.info("Request received", {
      sourceIp: event.requestContext?.identity?.sourceIp,
      userAgent: event.headers?.["User-Agent"] || event.headers?.["user-agent"],
      bodyLength: event.body?.length || 0,
    });

    const rawData = JSON.parse(event.body || "{}");

    logger.info("Request data parsed", {
      fieldsReceived: Object.keys(rawData),
      itemsCount: Array.isArray(rawData.items) ? rawData.items.length : 0,
    });

    const validation = validateCreateOrderData(rawData);
    if (!validation.isValid) {
      logger.warn("Validation failed", {
        error: validation.error,
        receivedFields: Object.keys(rawData),
      });
      tracker.finishWithError(new Error(validation.error!));
      return createErrorResponse(400, validation.error!);
    }

    const orderData = sanitizeCreateOrderData(rawData);

    const orderRequest = {
      orderId: `order-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
      cpf: orderData.cpf,
      email: orderData.email,
      name: orderData.name,
      items: orderData.items,
      timestamp: new Date().toISOString(),
      status: "requested",
    };

    const orderLogger = logger.withContext({ orderId: orderRequest.orderId });
    orderLogger.info("Order created", {
      customerCpf: maskSensitiveData.cpf(orderRequest.cpf),
      customerEmail: maskSensitiveData.email(orderRequest.email),
      customerName: maskSensitiveData.name(orderRequest.name),
      itemsCount: orderRequest.items.length,
      totalQuantity: orderRequest.items.reduce(
        (total, item) => total + item.quantity,
        0
      ),
    });

    const params = {
      TopicArn: process.env.INITIALIZE_ORDER_TOPIC_ARN!,
      Message: JSON.stringify(orderRequest),
      Subject: "New Order Request",
      MessageAttributes: {
        messageType: {
          DataType: "String",
          StringValue: "order_created",
        },
      },
    };

    const snsTracker = new PerformanceTracker(orderLogger, "sns-publish");
    await sns.publish(params).promise();
    snsTracker.finish({
      topicArn: process.env.INITIALIZE_ORDER_TOPIC_ARN,
      messageSize: JSON.stringify(orderRequest).length,
    });

    const response = createSuccessResponse(202, {
      message: "Order request submitted successfully",
      orderId: orderRequest.orderId,
      status: "submitted",
    });

    tracker.finish({
      statusCode: 202,
      orderId: orderRequest.orderId,
    });

    return response;
  } catch (error) {
    logger.error("Unexpected error occurred", error, {
      hasBody: !!event.body,
      bodyLength: event.body?.length || 0,
    });

    tracker.finishWithError(error);
    return createErrorResponse(500, "Internal server error");
  }
};
