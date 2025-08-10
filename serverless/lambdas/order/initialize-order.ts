import { APIGatewayProxyEvent, APIGatewayProxyResult } from "aws-lambda";
import { SNS } from "aws-sdk";
import {
  validateInitializeOrderData,
  sanitizeInitializeOrderData,
  createErrorResponse,
  createSuccessResponse,
  PaymentData,
} from "../../shared/validators";
import {
  createLogger,
  generateCorrelationId,
  maskSensitiveData,
  PerformanceTracker,
} from "../../shared/logger";
import {
  AddressData,
  CustomerData,
  OrderItem,
} from "../../shared/schemas/order";

export type BodyData = {
  paymentData: PaymentData;
  addressData: AddressData;
  customerData: CustomerData;
  items: Pick<OrderItem, "quantity" | "id">[];
};

export interface MessageData extends BodyData {
  orderId: string;
}

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

    const rawData = JSON.parse(event.body || "{}") as BodyData;

    logger.info("Request data parsed", {
      fieldsReceived: Object.keys(rawData),
      itemsCount: Array.isArray(rawData.items) ? rawData.items.length : 0,
    });

    const validation = validateInitializeOrderData(rawData);
    if (!validation.isValid) {
      logger.warn("Validation failed", {
        error: validation.error,
        receivedFields: Object.keys(rawData),
      });
      tracker.finishWithError(new Error(validation.error!));
      return createErrorResponse(400, validation.error!);
    }

    const orderData = sanitizeInitializeOrderData(rawData);

    const orderId = `order-${Date.now()}-${Math.random()
      .toString(36)
      .substr(2, 9)}`;

    const messageData: MessageData = {
      orderId,
      items: orderData.items,
      paymentData: orderData.paymentData,
      addressData: orderData.addressData,
      customerData: orderData.customerData,
    };

    const orderLogger = logger.withContext({ orderId: messageData.orderId });
    orderLogger.info("Message data mounted", {
      cpf: maskSensitiveData.cpf(messageData.customerData.cpf),
      email: maskSensitiveData.email(messageData.customerData.email),
      name: maskSensitiveData.name(messageData.customerData.name),
      itemsCount: messageData.items.length,
      totalQuantity: messageData.items.reduce(
        (total, item) => total + item.quantity,
        0
      ),
    });

    const params = {
      TopicArn: process.env.INITIALIZE_ORDER_TOPIC_ARN!,
      Message: JSON.stringify(messageData),
      Subject: "New Order Request",
      MessageAttributes: {
        messageType: {
          DataType: "String",
          StringValue: "order_initialized",
        },
      },
    };

    const snsTracker = new PerformanceTracker(orderLogger, "sns-publish");
    await sns.publish(params).promise();
    snsTracker.finish({
      topicArn: process.env.INITIALIZE_ORDER_TOPIC_ARN,
      messageSize: JSON.stringify(messageData).length,
    });

    const response = createSuccessResponse(202, {
      message: "Order request submitted successfully",
      orderId: messageData.orderId,
      status: "submitted",
    });

    tracker.finish({
      statusCode: 202,
      orderId: messageData.orderId,
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
