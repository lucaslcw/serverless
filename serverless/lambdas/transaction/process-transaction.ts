import { SQSEvent } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
} from "@aws-sdk/lib-dynamodb";
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import {
  createLogger,
  generateCorrelationId,
  maskSensitiveData,
  PerformanceTracker,
  StructuredLogger,
} from "../../shared/logger";
import {
  AddressData,
  CustomerData,
  OrderData,
  OrderStatus,
  PaymentData,
} from "../../shared/schemas/order";
import { PaymentStatus, TransactionData } from "../../shared/schemas/transaction";

const dynamoClient = new DynamoDBClient({ region: process.env.AWS_REGION || 'us-east-1' });
const docClient = DynamoDBDocumentClient.from(dynamoClient);
const sqsClient = new SQSClient({ region: process.env.AWS_REGION || 'us-east-1' });

const ORDER_TABLE_NAME = process.env.ORDER_COLLECTION_TABLE!;
const TRANSACTION_TABLE_NAME = process.env.TRANSACTION_COLLECTION_TABLE!;
const UPDATE_ORDER_QUEUE_URL = process.env.UPDATE_ORDER_QUEUE_URL!;

const PAYMENT_LIMITS = {
  HIGH_VALUE_THRESHOLD: 10000,
  MEDIUM_VALUE_THRESHOLD: 1000,
} as const;

const APPROVAL_RATES = {
  HIGH_VALUE: 0.75,   // 75% approval rate for high-value transactions
  MEDIUM_VALUE: 0.85, // 85% approval rate for medium-value transactions  
  LOW_VALUE: 0.95,    // 95% approval rate for low-value transactions
} as const;

const GATEWAY_CONFIG = {
  BASE_DELAY_MS: 200,
  MAX_ADDITIONAL_DELAY_MS: 500,
  FAILURE_RATE: 0.03, // 3% gateway failure rate
} as const;

export interface TransactionMessage {
  orderId: string;
  orderTotalValue: number;
  paymentData: PaymentData;
  addressData: AddressData;
  customerData: CustomerData;
}

type TransactionValueCategory = 'HIGH' | 'MEDIUM' | 'LOW';

interface PaymentResult {
  status: PaymentStatus;
  processingTime: number;
  authCode?: string;
  errorMessage?: string;
}

function validateTransactionMessage(message: any): TransactionMessage {
  const requiredFields = ['orderId', 'orderTotalValue', 'paymentData', 'addressData', 'customerData'];
  const missingFields = requiredFields.filter(field => !message[field]);
  
  if (missingFields.length > 0) {
    throw new Error(`Missing required fields: ${missingFields.join(', ')}`);
  }

  const paymentRequiredFields = ['cardNumber', 'cvv', 'expiryMonth', 'expiryYear', 'cardHolderName'];
  const missingPaymentFields = paymentRequiredFields.filter(field => !message.paymentData[field]);
  
  if (missingPaymentFields.length > 0) {
    throw new Error(`Missing payment data fields: ${missingPaymentFields.join(', ')}`);
  }

  return message as TransactionMessage;
}

function getTransactionValueCategory(totalValue: number): TransactionValueCategory {
  if (totalValue >= PAYMENT_LIMITS.HIGH_VALUE_THRESHOLD) {
    return 'HIGH';
  }
  if (totalValue >= PAYMENT_LIMITS.MEDIUM_VALUE_THRESHOLD) {
    return 'MEDIUM';
  }
  return 'LOW';
}

function generateTransactionId(): string {
  const timestamp = Date.now();
  const randomPart = Math.random().toString(36).substring(2, 15);
  return `txn-${timestamp}-${randomPart}`;
}

export const handler = async (event: SQSEvent): Promise<void> => {
  const processId = generateCorrelationId("transaction");
  const logger = createLogger({
    processId,
    functionName: "process-transaction",
  });

  const batchTracker = new PerformanceTracker(logger, "transaction-batch");

  logger.info("Processing transaction batch started", {
    recordsCount: event.Records.length,
  });

  for (let i = 0; i < event.Records.length; i++) {
    const record = event.Records[i];
    const recordLogger = logger.withContext({
      recordIndex: i,
      messageId: record.messageId,
    });

    const recordTracker = new PerformanceTracker(recordLogger, "transaction-record");

    try {
      recordLogger.info("Processing transaction record", {
        eventSource: record.eventSource,
        receiptHandle: record.receiptHandle?.substring(0, 30) + "...",
      });

      const rawMessage = JSON.parse(record.body);
      const transactionMessage = validateTransactionMessage(rawMessage);

      await processTransaction(transactionMessage, recordLogger);

      recordTracker.finish({
        orderId: transactionMessage.orderId,
        status: "success",
      });

    } catch (error: any) {
      recordLogger.error("Error processing transaction record", error, {
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

  logger.info("Transaction batch processing completed", {
    totalRecords: event.Records.length,
  });
};

async function processTransaction(
  message: TransactionMessage,
  logger: StructuredLogger
): Promise<void> {
  const transactionLogger = logger.withContext({ orderId: message.orderId });
  const processingTracker = new PerformanceTracker(transactionLogger, "transaction-processing");

  transactionLogger.info("Starting transaction processing", {
    orderId: message.orderId,
    amount: message.orderTotalValue,
    valueCategory: getTransactionValueCategory(message.orderTotalValue),
    cardLastFour: message.paymentData.cardNumber.slice(-4),
    customerName: maskSensitiveData.name(message.customerData.name),
    addressCity: message.addressData.city,
    addressState: message.addressData.state,
  });

  try {
    const order = await getOrderById(message.orderId, transactionLogger);
    if (!order) {
      throw new Error(`Order not found: ${message.orderId}`);
    }

    transactionLogger.info("Order found for processing", {
      orderId: message.orderId,
      currentStatus: order.status,
      totalValue: order.totalValue,
      leadId: order.leadId,
    });

    const paymentResult = await processPayment(message.paymentData, message.orderTotalValue, transactionLogger);
    
    const transactionId = generateTransactionId();
    await createTransactionRecord(
      transactionId,
      message.orderId,
      message.orderTotalValue,
      message.paymentData,
      message.addressData,
      message.customerData,
      paymentResult,
      transactionLogger
    );

    const finalOrderStatus = paymentResult.status === PaymentStatus.APPROVED 
      ? OrderStatus.PROCESSED 
      : OrderStatus.CANCELLED;
    
    const statusReason = paymentResult.status === PaymentStatus.APPROVED
      ? `Payment approved: ${paymentResult.authCode}`
      : `Payment ${paymentResult.status.toLowerCase()}: ${paymentResult.errorMessage || 'Processing failed'}`;

    await sendOrderUpdateMessage(
      message.orderId,
      finalOrderStatus,
      statusReason,
      transactionId,
      transactionLogger
    );

    processingTracker.finish({
      orderId: message.orderId,
      transactionId,
      paymentStatus: paymentResult.status,
      amount: message.orderTotalValue,
      processingTime: paymentResult.processingTime,
    });

    transactionLogger.info("Transaction processing completed successfully", {
      orderId: message.orderId,
      transactionId,
      paymentStatus: paymentResult.status,
      finalOrderStatus,
    });

  } catch (error: any) {
    transactionLogger.error("Transaction processing failed", error, {
      orderId: message.orderId,
      amount: message.orderTotalValue,
    });

    try {
      const errorTransactionId = generateTransactionId();
      await createTransactionRecord(
        errorTransactionId,
        message.orderId,
        message.orderTotalValue,
        message.paymentData,
        message.addressData,
        message.customerData,
        {
          status: PaymentStatus.ERROR,
          processingTime: 0,
          errorMessage: error.message,
        },
        transactionLogger
      );

      await sendOrderUpdateMessage(
        message.orderId,
        OrderStatus.CANCELLED,
        `Payment processing error: ${error.message}`,
        errorTransactionId,
        transactionLogger
      );
    } catch (recordError) {
      transactionLogger.error("Failed to create error transaction record", recordError, {
        orderId: message.orderId,
      });
    }

    processingTracker.finishWithError(error);
    throw error;
  }
}

async function processPayment(
  paymentData: PaymentData,
  totalValue: number,
  logger: StructuredLogger
): Promise<PaymentResult> {
  const paymentTracker = new PerformanceTracker(logger, "payment-gateway-call");
  const startTime = Date.now();

  logger.info("Processing payment with gateway", {
    amount: totalValue,
    valueCategory: getTransactionValueCategory(totalValue),
    cardLastFour: paymentData.cardNumber.slice(-4),
    cardHolderName: maskSensitiveData.name(paymentData.cardHolderName),
  });

  try {
    await simulatePaymentGatewayCall();

    const isApproved = simulatePaymentApproval(paymentData, totalValue);
    const processingTime = Date.now() - startTime;

    const result: PaymentResult = {
      status: isApproved ? PaymentStatus.APPROVED : PaymentStatus.DECLINED,
      processingTime,
      authCode: isApproved ? `AUTH-${Date.now()}` : undefined,
    };

    paymentTracker.finish({
      status: result.status,
      processingTime: result.processingTime,
      amount: totalValue,
    });

    logger.info("Payment gateway response received", {
      status: result.status,
      processingTime: result.processingTime,
      authCode: result.authCode,
      amount: totalValue,
    });

    return result;

  } catch (error: any) {
    const processingTime = Date.now() - startTime;
    
    logger.error("Payment gateway call failed", error, {
      amount: totalValue,
      processingTime,
    });

    paymentTracker.finishWithError(error);

    return {
      status: PaymentStatus.ERROR,
      processingTime,
      errorMessage: error.message,
    };
  }
}

async function simulatePaymentGatewayCall(): Promise<void> {
  const totalDelay = GATEWAY_CONFIG.BASE_DELAY_MS + 
    (Math.random() * GATEWAY_CONFIG.MAX_ADDITIONAL_DELAY_MS);
  
  await new Promise((resolve) => setTimeout(resolve, totalDelay));

  if (Math.random() < GATEWAY_CONFIG.FAILURE_RATE) {
    const errorTypes = [
      "Payment gateway timeout",
      "Payment gateway service unavailable", 
      "Invalid merchant configuration",
      "Network connection error"
    ];
    const randomError = errorTypes[Math.floor(Math.random() * errorTypes.length)];
    throw new Error(randomError);
  }
}

function simulatePaymentApproval(paymentData: PaymentData, totalValue: number): boolean {
  if (paymentData.cardNumber.endsWith("0000")) {
    return false;
  }

  const valueCategory = getTransactionValueCategory(totalValue);
  let approvalRate: number;
  
  switch (valueCategory) {
    case 'HIGH':
      approvalRate = APPROVAL_RATES.HIGH_VALUE;
      break;
    case 'MEDIUM':
      approvalRate = APPROVAL_RATES.MEDIUM_VALUE;
      break;
    case 'LOW':
      approvalRate = APPROVAL_RATES.LOW_VALUE;
      break;
  }

  return Math.random() < approvalRate;
}

async function getOrderById(orderId: string, logger: StructuredLogger): Promise<OrderData | null> {
  const orderTracker = new PerformanceTracker(logger, "order-lookup");

  try {
    logger.info("Looking up order", { orderId });

    const getCommand = new GetCommand({
      TableName: ORDER_TABLE_NAME,
      Key: { orderId },
    });

    const result = await docClient.send(getCommand);

    if (result.Item) {
      const order = result.Item as OrderData;
      
      orderTracker.finish({
        orderId,
        found: true,
        status: order.status,
      });

      logger.info("Order found", {
        orderId,
        status: order.status,
        totalValue: order.totalValue,
        leadId: order.leadId,
      });

      return order;
    } else {
      orderTracker.finish({
        orderId,
        found: false,
      });

      logger.warn("Order not found", { orderId });
      return null;
    }
  } catch (error) {
    logger.error("Error looking up order", error, { orderId });
    orderTracker.finishWithError(error);
    throw error;
  }
}

async function createTransactionRecord(
  transactionId: string,
  orderId: string,
  totalValue: number,
  paymentData: PaymentData,
  addressData: AddressData,
  customerData: CustomerData,
  paymentResult: PaymentResult,
  logger: StructuredLogger
): Promise<void> {
  const transactionTracker = new PerformanceTracker(logger, "transaction-record-creation");

  try {
    logger.info("Creating transaction record", {
      transactionId,
      orderId,
      paymentStatus: paymentResult.status,
      amount: totalValue,
    });

    const maskedPaymentData = {
      ...paymentData,
      cardNumber: `****-****-****-${paymentData.cardNumber.slice(-4)}`,
      cvv: "***",
    };

    const transactionRecord: TransactionData = {
      id: transactionId,
      orderId,
      paymentData: maskedPaymentData,
      addressData,
      customerData: {
        name: customerData.name,
        email: customerData.email,
        cpf: maskSensitiveData.cpf(customerData.cpf),
      },
      paymentStatus: paymentResult.status,
      amount: totalValue,
      authCode: paymentResult.authCode,
      processingTime: paymentResult.processingTime,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };

    const putCommand = new PutCommand({
      TableName: TRANSACTION_TABLE_NAME,
      Item: transactionRecord,
      ConditionExpression: "attribute_not_exists(id)",
    });

    await docClient.send(putCommand);

    transactionTracker.finish({
      transactionId,
      orderId,
      paymentStatus: paymentResult.status,
      amount: totalValue,
    });

    logger.info("Transaction record created successfully", {
      transactionId,
      orderId,
      paymentStatus: paymentResult.status,
      amount: totalValue,
      processingTime: paymentResult.processingTime,
    });

  } catch (error: any) {
    logger.error("Failed to create transaction record", error, {
      transactionId,
      orderId,
      paymentStatus: paymentResult.status,
    });

    transactionTracker.finishWithError(error);
    throw error;
  }
}

async function sendOrderUpdateMessage(
  orderId: string,
  status: OrderStatus,
  reason: string,
  transactionId: string,
  logger: StructuredLogger
): Promise<void> {
  const messageTracker = new PerformanceTracker(logger, "order-update-message");

  try {
    logger.info("Sending order update message", {
      orderId,
      status,
      reason,
      transactionId,
    });

    const updateMessage = {
      orderId,
      status,
      reason,
      transactionId,
    };

    const sendCommand = new SendMessageCommand({
      QueueUrl: UPDATE_ORDER_QUEUE_URL,
      MessageBody: JSON.stringify(updateMessage),
      MessageAttributes: {
        orderId: {
          DataType: "String",
          StringValue: orderId,
        },
        status: {
          DataType: "String",
          StringValue: status,
        },
        transactionId: {
          DataType: "String",
          StringValue: transactionId,
        },
      },
    });

    const result = await sqsClient.send(sendCommand);

    messageTracker.finish({
      orderId,
      status,
      messageId: result.MessageId,
    });

    logger.info("Order update message sent successfully", {
      orderId,
      status,
      reason,
      transactionId,
      messageId: result.MessageId,
    });

  } catch (error: any) {
    logger.error("Failed to send order update message", error, {
      orderId,
      status,
      transactionId,
    });

    messageTracker.finishWithError(error);
    throw error;
  }
}
