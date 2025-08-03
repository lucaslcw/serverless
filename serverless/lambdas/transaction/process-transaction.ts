import { SQSEvent } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  GetCommand,
  UpdateCommand,
  PutCommand,
} from "@aws-sdk/lib-dynamodb";
import {
  createLogger,
  generateCorrelationId,
  maskSensitiveData,
  PerformanceTracker,
} from "../../shared/logger";

const dynamoClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const docClient = DynamoDBDocumentClient.from(dynamoClient);

const ORDER_TABLE_NAME = process.env.ORDER_COLLECTION_TABLE!;
const TRANSACTION_TABLE_NAME = process.env.TRANSACTION_COLLECTION_TABLE!;

enum PaymentStatus {
  PENDING = "PENDING",
  APPROVED = "APPROVED",
  DECLINED = "DECLINED",
  ERROR = "ERROR"
}

enum OrderStatus {
  PENDING = "PENDING",
  PROCESSED = "PROCESSED",
  PAYMENT_APPROVED = "PAYMENT_APPROVED",
  PAYMENT_DECLINED = "PAYMENT_DECLINED"
}

interface PaymentData {
  cardNumber: string;
  cardHolderName: string;
  expiryMonth: string;
  expiryYear: string;
  cvv: string;
  amount: number;
}

interface AddressData {
  street: string;
  number: string;
  complement?: string;
  neighborhood: string;
  city: string;
  state: string;
  zipCode: string;
  country: string;
}

interface TransactionMessage {
  orderId: string;
  paymentData: PaymentData;
  addressData: AddressData;
  customerInfo?: {
    name: string;
    email: string;
    cpf: string;
  };
}

interface OrderRecord {
  orderId: string;
  leadId: string;
  customerCpf: string;
  customerEmail: string;
  customerName: string;
  items: any[];
  totalItems: number;
  totalValue: number;
  status: OrderStatus;
  source: string;
  createdAt: string;
  updatedAt: string;
}

interface TransactionRecord {
  transactionId: string;
  orderId: string;
  paymentData: PaymentData;
  addressData: AddressData;
  customerInfo: {
    name: string;
    email: string;
    cpf: string;
  };
  paymentStatus: PaymentStatus;
  amount: number;
  authCode?: string;
  processingTime?: number;
  source: string;
  createdAt: string;
  updatedAt: string;
}

export const handler = async (event: SQSEvent): Promise<void> => {
  const processId = generateCorrelationId("transaction");
  const logger = createLogger({
    processId,
    functionName: "process-transaction",
  });

  const mainTracker = new PerformanceTracker(logger, "transaction-batch");

  logger.info("Processing transaction batch started", {
    recordsCount: event.Records.length,
  });

  for (const [index, record] of event.Records.entries()) {
    const recordLogger = logger.withContext({
      recordIndex: index,
      messageId: record.messageId,
    });

    const recordTracker = new PerformanceTracker(
      recordLogger,
      "transaction-record"
    );

    recordLogger.info("Processing SQS record", {
      receiptHandle: record.receiptHandle.substring(0, 50) + "...",
      body: record.body.substring(0, 200) + "...",
    });

    try {
      const message: TransactionMessage = JSON.parse(record.body);

      const transactionLogger = recordLogger.withContext({
        orderId: message.orderId,
      });

      transactionLogger.info("Transaction message parsed successfully", {
        orderId: message.orderId,
        amount: message.paymentData.amount,
        cardLastFour: message.paymentData.cardNumber.slice(-4),
        customerName: maskSensitiveData.name(message.customerInfo?.name || ""),
        addressCity: message.addressData.city,
        addressState: message.addressData.state,
      });

      const processingTracker = new PerformanceTracker(
        transactionLogger,
        "transaction-processing-logic"
      );

      transactionLogger.info("Starting transaction processing");

      await processTransaction(message, transactionLogger);

      processingTracker.finish({
        orderId: message.orderId,
        amount: message.paymentData.amount,
      });

      transactionLogger.info("Transaction processing completed successfully", {
        orderId: message.orderId,
        amount: message.paymentData.amount,
      });

      recordTracker.finish({
        orderId: message.orderId,
        status: "success",
      });
    } catch (error) {
      recordLogger.error("Error processing transaction record", error, {
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

  logger.info("Transaction batch processing completed", {
    totalRecords: event.Records.length,
  });
};

async function processTransaction(
  message: TransactionMessage,
  logger: any
): Promise<void> {
  const transactionTracker = new PerformanceTracker(
    logger,
    "payment-processing"
  );

  try {
    logger.info("Starting payment processing", {
      orderId: message.orderId,
      amount: message.paymentData.amount,
      cardLastFour: message.paymentData.cardNumber.slice(-4),
      cardHolderName: maskSensitiveData.name(message.paymentData.cardHolderName),
    });

    const order = await getOrderById(message.orderId, logger);

    if (!order) {
      const error = new Error(`Order not found: ${message.orderId}`);
      logger.error("Order not found for transaction processing", error, {
        orderId: message.orderId,
      });
      throw error;
    }

    logger.info("Order found for transaction processing", {
      orderId: message.orderId,
      currentStatus: order.status,
      totalValue: order.totalValue,
      leadId: order.leadId,
    });

    const paymentResult = await processPayment(message.paymentData, logger);

    const transactionId = `txn-${Date.now()}-${Math.random().toString(36).substring(2, 15)}`;

    if (!message.customerInfo) {
      logger.error("Missing customer info in transaction message", null, {
        orderId: message.orderId,
      });
      throw new Error("Customer info is required for transaction processing");
    }

    await createTransactionRecord(
      transactionId,
      message.orderId,
      message.paymentData,
      message.addressData,
      message.customerInfo,
      paymentResult,
      logger
    );

    await updateOrderStatus(
      message.orderId,
      paymentResult.status === PaymentStatus.APPROVED ? OrderStatus.PAYMENT_APPROVED : OrderStatus.PAYMENT_DECLINED,
      logger
    );

    transactionTracker.finish({
      orderId: message.orderId,
      transactionId: transactionId,
      paymentStatus: paymentResult.status,
      amount: message.paymentData.amount,
    });

    logger.info("Payment processing completed successfully", {
      orderId: message.orderId,
      transactionId: transactionId,
      paymentStatus: paymentResult.status,
      amount: message.paymentData.amount,
      processingTime: paymentResult.processingTime,
    });

  } catch (error: any) {
    logger.error("Failed to process payment transaction", error, {
      orderId: message.orderId,
      amount: message.paymentData.amount,
      errorName: error.name,
    });

    try {
      const transactionId = `txn-${Date.now()}-${Math.random().toString(36).substring(2, 15)}`;
      
      const defaultCustomerInfo = message.customerInfo || {
        name: "Unknown Customer",
        email: "unknown@domain.com",
        cpf: "000.000.000-00"
      };
      
      await createTransactionRecord(
        transactionId,
        message.orderId,
        message.paymentData,
        message.addressData,
        defaultCustomerInfo,
        { status: PaymentStatus.ERROR, processingTime: 0 },
        logger
      );
      
      await updateOrderStatus(message.orderId, OrderStatus.PENDING, logger);
    } catch (updateError) {
      logger.error("Failed to create error transaction record", updateError, {
        orderId: message.orderId,
      });
    }

    transactionTracker.finishWithError(error);
    throw error;
  }
}

async function processPayment(
  paymentData: PaymentData,
  logger: any
): Promise<{ status: PaymentStatus; processingTime: number; authCode?: string }> {
  const paymentTracker = new PerformanceTracker(
    logger,
    "payment-gateway-call"
  );

  const startTime = Date.now();

  try {
    logger.info("Processing payment with gateway", {
      amount: paymentData.amount,
      cardLastFour: paymentData.cardNumber.slice(-4),
      cardHolderName: maskSensitiveData.name(paymentData.cardHolderName),
    });

    await simulatePaymentGatewayCall(paymentData);

    const isApproved = simulatePaymentApproval(paymentData);
    const processingTime = Date.now() - startTime;

    const result = {
      status: isApproved ? PaymentStatus.APPROVED : PaymentStatus.DECLINED,
      processingTime,
      authCode: isApproved ? `AUTH-${Date.now()}` : undefined,
    };

    paymentTracker.finish({
      status: result.status,
      processingTime: result.processingTime,
      amount: paymentData.amount,
    });

    logger.info("Payment gateway response received", {
      status: result.status,
      processingTime: result.processingTime,
      authCode: result.authCode,
      amount: paymentData.amount,
    });

    return result;

  } catch (error) {
    const processingTime = Date.now() - startTime;
    
    logger.error("Payment gateway call failed", error, {
      amount: paymentData.amount,
      processingTime: processingTime,
    });

    paymentTracker.finishWithError(error);

    return {
      status: PaymentStatus.ERROR,
      processingTime,
    };
  }
}

async function simulatePaymentGatewayCall(paymentData: PaymentData): Promise<void> {
  const delay = Math.random() * 1000 + 500;
  await new Promise(resolve => setTimeout(resolve, delay));

  if (Math.random() < 0.05) {
    throw new Error("Payment gateway timeout");
  }
}

function simulatePaymentApproval(paymentData: PaymentData): boolean {
  if (paymentData.cardNumber.endsWith("0000")) {
    return false;
  }

  if (paymentData.amount > 10000) {
    return Math.random() > 0.2;
  }

  return Math.random() > 0.1;
}

async function getOrderById(
  orderId: string,
  logger: any
): Promise<OrderRecord | null> {
  const orderTracker = new PerformanceTracker(
    logger,
    "order-lookup"
  );

  try {
    logger.info("Looking up order for transaction", {
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
      const order: OrderRecord = result.Item as OrderRecord;

      logger.info("Order found for transaction", {
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
      logger.warn("Order not found for transaction", {
        orderId: orderId,
      });

      orderTracker.finish({
        orderId: orderId,
        found: false,
      });

      return null;
    }
  } catch (error) {
    logger.error("Error looking up order for transaction", error, {
      orderId: orderId,
    });

    orderTracker.finishWithError(error);
    throw error;
  }
}

async function createTransactionRecord(
  transactionId: string,
  orderId: string,
  paymentData: PaymentData,
  addressData: AddressData,
  customerInfo: { name: string; email: string; cpf: string },
  paymentResult: { status: PaymentStatus; processingTime: number; authCode?: string },
  logger: any
): Promise<void> {
  const transactionTracker = new PerformanceTracker(
    logger,
    "transaction-record-creation"
  );

  try {
    logger.info("Creating transaction record", {
      transactionId: transactionId,
      orderId: orderId,
      paymentStatus: paymentResult.status,
      amount: paymentData.amount,
    });

    const maskedPaymentData = {
      ...paymentData,
      cardNumber: `****-****-****-${paymentData.cardNumber.slice(-4)}`,
      cvv: "***",
    };

    const transactionRecord: TransactionRecord = {
      transactionId: transactionId,
      orderId: orderId,
      paymentData: maskedPaymentData,
      addressData: addressData,
      customerInfo: {
        name: customerInfo.name,
        email: customerInfo.email,
        cpf: maskSensitiveData.cpf(customerInfo.cpf),
      },
      paymentStatus: paymentResult.status,
      amount: paymentData.amount,
      authCode: paymentResult.authCode,
      processingTime: paymentResult.processingTime,
      source: "payment-processing-service",
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };

    const putCommand = new PutCommand({
      TableName: TRANSACTION_TABLE_NAME,
      Item: transactionRecord,
      ConditionExpression: "attribute_not_exists(transactionId)",
    });

    await docClient.send(putCommand);

    transactionTracker.finish({
      transactionId: transactionId,
      orderId: orderId,
      paymentStatus: paymentResult.status,
      amount: paymentData.amount,
    });

    logger.info("Transaction record created successfully", {
      transactionId: transactionId,
      orderId: orderId,
      paymentStatus: paymentResult.status,
      amount: paymentData.amount,
      authCode: paymentResult.authCode,
      processingTime: paymentResult.processingTime,
      cardLastFour: paymentData.cardNumber.slice(-4),
      addressCity: addressData.city,
      addressState: addressData.state,
    });

  } catch (error: any) {
    logger.error("Failed to create transaction record", error, {
      transactionId: transactionId,
      orderId: orderId,
      paymentStatus: paymentResult.status,
      errorName: error.name,
    });

    transactionTracker.finishWithError(error);
    throw error;
  }
}

async function updateOrderStatus(
  orderId: string,
  newStatus: OrderStatus,
  logger: any
): Promise<void> {
  const updateTracker = new PerformanceTracker(
    logger,
    "order-status-update"
  );

  try {
    logger.info("Updating order status", {
      orderId: orderId,
      newStatus: newStatus,
    });

    const updateCommand = new UpdateCommand({
      TableName: ORDER_TABLE_NAME,
      Key: {
        orderId: orderId,
      },
      UpdateExpression: "SET #status = :status, updatedAt = :updatedAt",
      ExpressionAttributeNames: {
        "#status": "status",
      },
      ExpressionAttributeValues: {
        ":status": newStatus,
        ":updatedAt": new Date().toISOString(),
      },
      ReturnValues: "ALL_NEW",
    });

    const result = await docClient.send(updateCommand);

    updateTracker.finish({
      orderId: orderId,
      newStatus: newStatus,
    });

    logger.info("Order status updated successfully", {
      orderId: orderId,
      previousStatus: "PENDING",
      newStatus: newStatus,
    });

  } catch (error: any) {
    logger.error("Failed to update order status", error, {
      orderId: orderId,
      newStatus: newStatus,
      errorName: error.name,
    });

    updateTracker.finishWithError(error);
    throw error;
  }
}
