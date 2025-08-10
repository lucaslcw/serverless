import { SQSEvent } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import {
  DynamoDBDocumentClient,
  QueryCommand,
  PutCommand,
  GetCommand,
} from "@aws-sdk/lib-dynamodb";
import {
  createLogger,
  generateCorrelationId,
  maskSensitiveData,
  PerformanceTracker,
  StructuredLogger,
} from "../../shared/logger";
import { MessageData } from "./initialize-order";
import {
  AddressData,
  CustomerData,
  OrderData,
  OrderItem,
  OrderStatus,
  PaymentData,
} from "../../shared/schemas/order";
import { TransactionMessage } from "../transaction/process-transaction";
import { LeadData } from "../../shared/schemas/lead";
import { ProductData } from "../../shared/schemas/product";
import { StockOperationType, ProductStockData } from "../../shared/schemas/product-stock";
import { StockUpdateMessage } from "../product/update-product-stock";

const dynamoClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const sqsClient = new SQSClient({ region: process.env.AWS_REGION });
const docClient = DynamoDBDocumentClient.from(dynamoClient);

const LEAD_TABLE_NAME = process.env.LEAD_COLLECTION_TABLE!;
const ORDER_TABLE_NAME = process.env.ORDER_COLLECTION_TABLE!;
const PRODUCT_TABLE_NAME = process.env.PRODUCT_COLLECTION_TABLE!;
const PRODUCT_STOCK_TABLE_NAME = "product-stock";
const PRODUCT_STOCK_QUEUE_URL = process.env.PRODUCT_STOCK_QUEUE_URL!;
const PROCESS_TRANSACTION_QUEUE_URL =
  process.env.PROCESS_TRANSACTION_QUEUE_URL!;

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

type ExistingLead = Pick<
  LeadData,
  "id" | "cpf" | "name" | "email" | "createdAt" | "updatedAt"
>;

export const handler = async (event: SQSEvent): Promise<void> => {
  const processId = generateCorrelationId("order");
  const logger = createLogger({
    processId,
    functionName: "create-order",
  });

  const mainTracker = new PerformanceTracker(logger, "create-order-batch");

  logger.info("Processing SQS batch started", {
    recordsCount: event.Records.length,
  });

  for (const [index, record] of event.Records.entries()) {
    const recordLogger = logger.withContext({
      recordIndex: index,
      messageId: record.messageId,
    });

    const recordTracker = new PerformanceTracker(
      recordLogger,
      "create-order-record"
    );

    recordLogger.info("Processing SQS record", {
      receiptHandle: record.receiptHandle.substring(0, 50) + "...",
      body: record.body.substring(0, 200) + "...",
    });

    try {
      const snsMessage = JSON.parse(record.body);
      const message: MessageData = JSON.parse(snsMessage.Message);

      const orderLogger = recordLogger.withContext({
        orderId: message.orderId,
      });

      orderLogger.info("Order data parsed successfully", {
        cpf: maskSensitiveData.cpf(message?.customerData?.cpf || ""),
        email: maskSensitiveData.email(message?.customerData?.email || ""),
        name: maskSensitiveData.name(message?.customerData?.name || ""),
        itemsCount: message?.items ? message.items.length : 0,
      });

      const processingTracker = new PerformanceTracker(
        orderLogger,
        "order-creation-logic"
      );

      orderLogger.info("Starting order creation logic");

      const enrichedItems = await enrichOrderItems(
        message.items || [],
        orderLogger
      );

      const totalValue = enrichedItems.reduce(
        (total: number, item: OrderItem) => total + (item.totalPrice || 0),
        0
      );

      try {
        await sendStockUpdateMessages(
          enrichedItems,
          message.orderId,
          orderLogger
        );

        const leadData = await findOrCreateLead(
          message.customerData?.email,
          message.customerData?.cpf,
          message.customerData?.name,
          message.orderId,
          orderLogger
        );

        const orderRecord: OrderData = {
          id: message.orderId,
          cpf: leadData.cpf,
          name: leadData.name,
          leadId: leadData.id,
          email: leadData.email,
          items: enrichedItems,
          totalItems: enrichedItems.reduce(
            (total: number, item: OrderItem) => total + item.quantity,
            0
          ),
          totalValue: totalValue,
          status: OrderStatus.PENDING,
          addressData: message.addressData,
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
        };

        await createOrderInDatabase(orderRecord, orderLogger);

        if (message.paymentData && message.addressData) {
          await sendTransactionMessage(
            message.orderId,
            orderRecord.totalValue,
            message.paymentData,
            message.addressData,
            message.customerData,
            orderLogger
          );
        }

        processingTracker.finish({
          newStatus: orderRecord.status,
          totalItems: orderRecord.totalItems,
          totalValue: orderRecord.totalValue,
          leadId: leadData.id,
          leadAction: "associated",
        });

        orderLogger.info("Order created successfully", {
          newStatus: orderRecord.status,
          totalItems: orderRecord.totalItems,
          totalValue: orderRecord.totalValue,
          leadId: leadData.id,
        });
      } catch (orderError) {
        orderLogger.warn(
          "Order creation failed, stock messages may need to be handled by retry logic",
          {
            orderId: message.orderId,
          }
        );

        throw orderError;
      }

      recordTracker.finish({
        orderId: message.orderId,
        status: "success",
      });
    } catch (error) {
      recordLogger.error("Error processing order record", error, {
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

  logger.info("SQS batch processing completed", {
    totalRecords: event.Records.length,
  });
};

async function findOrCreateLead(
  email: string,
  cpf: string,
  name: string,
  orderReference: string,
  logger: StructuredLogger
): Promise<LeadData> {
  const leadTracker = new PerformanceTracker(logger, "find-or-create-lead");

  try {
    logger.info("Searching for existing lead or creating new one", {
      cpf: maskSensitiveData.cpf(cpf),
      email: maskSensitiveData.email(email),
      orderReference: orderReference,
    });

    const existingLead = await findExistingLead(email, cpf, logger);

    if (existingLead) {
      logger.info("Found existing lead, using existing leadId", {
        existingLeadId: existingLead.id,
        cpf: maskSensitiveData.cpf(cpf),
        email: maskSensitiveData.email(email),
      });

      leadTracker.finish({
        action: "found_existing_lead",
        leadId: existingLead.id,
      });

      return existingLead;
    }

    const newLeadId = `lead-${Date.now()}-${Math.random()
      .toString(36)
      .substring(2, 15)}`;

    const newLeadData: LeadData = {
      id: newLeadId,
      cpf: cpf,
      name: name,
      email: email,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };

    await createLeadInDatabase(newLeadData, logger);

    logger.info("Created new lead for order", {
      newLeadId: newLeadId,
      cpf: maskSensitiveData.cpf(cpf),
      email: maskSensitiveData.email(email),
      orderReference: orderReference,
    });

    leadTracker.finish({
      action: "created_new_lead",
      leadId: newLeadId,
    });

    return newLeadData;
  } catch (error) {
    logger.error("Error finding or creating lead", error, {
      cpf: maskSensitiveData.cpf(cpf || ""),
      email: maskSensitiveData.email(email || ""),
      orderReference: orderReference,
    });

    leadTracker.finishWithError(error);
    throw error;
  }
}
async function findExistingLead(
  email: string,
  cpf: string,
  logger: StructuredLogger
): Promise<ExistingLead | null> {
  const searchTracker = new PerformanceTracker(logger, "lead-search-query");

  try {
    logger.info("Searching for existing lead", {
      cpf: maskSensitiveData.cpf(cpf),
      email: maskSensitiveData.email(email),
    });

    const emailQueryCommand = new QueryCommand({
      TableName: LEAD_TABLE_NAME,
      IndexName: "email-index",
      KeyConditionExpression: "email = :email",
      ExpressionAttributeValues: {
        ":email": email,
      },
      ProjectionExpression:
        "id, leadId, cpf, email, name, createdAt, updatedAt",
    });

    const emailResult = await docClient.send(emailQueryCommand);

    if (emailResult.Items && emailResult.Items.length > 0) {
      const matchingLead = emailResult.Items.find((item) => item.cpf === cpf);

      if (matchingLead) {
        logger.info("Found existing lead with matching email and CPF", {
          existingLeadId: matchingLead.id,
          leadId: matchingLead.leadId,
          cpf: maskSensitiveData.cpf(cpf),
          email: maskSensitiveData.email(email),
        });

        searchTracker.finish({
          action: "found_existing_lead",
          leadId: matchingLead.leadId,
        });

        return {
          id: matchingLead.id as string,
          cpf: matchingLead.cpf as string,
          email: matchingLead.email as string,
          name: matchingLead.name as string,
          createdAt: matchingLead.createdAt as string,
          updatedAt: matchingLead.updatedAt as string,
        };
      }
    }

    logger.info("No existing lead found for email and CPF combination", {
      cpf: maskSensitiveData.cpf(cpf),
      email: maskSensitiveData.email(email),
      emailLeadsCount: emailResult.Items?.length || 0,
    });

    searchTracker.finish({
      action: "no_lead_found",
      emailLeadsCount: emailResult.Items?.length || 0,
    });

    return null;
  } catch (error) {
    logger.error("Error searching for existing lead", error, {
      cpf: maskSensitiveData.cpf(cpf || ""),
      email: maskSensitiveData.email(email || ""),
    });

    searchTracker.finishWithError(error);
    throw error;
  }
}

async function createLeadInDatabase(
  leadData: LeadData,
  logger: StructuredLogger
): Promise<void> {
  const creationTracker = new PerformanceTracker(
    logger,
    "dynamodb-lead-creation"
  );

  try {
    logger.info("Creating new lead in DynamoDB", {
      leadId: leadData.id,
      cpf: maskSensitiveData.cpf(leadData.cpf),
      email: maskSensitiveData.email(leadData.email),
    });

    const putCommand = new PutCommand({
      TableName: LEAD_TABLE_NAME,
      Item: leadData,
      ConditionExpression: "attribute_not_exists(id)",
    });

    await docClient.send(putCommand);

    logger.info("Lead successfully created in DynamoDB", {
      leadId: leadData.id,
      cpf: maskSensitiveData.cpf(leadData.cpf),
      email: maskSensitiveData.email(leadData.email),
    });

    creationTracker.finish({
      leadId: leadData.id,
      action: "lead_created",
    });
  } catch (error) {
    logger.error("Failed to create lead in DynamoDB", error, {
      leadId: leadData.id,
      cpf: maskSensitiveData.cpf(leadData.cpf || ""),
      email: maskSensitiveData.email(leadData.email || ""),
    });

    creationTracker.finishWithError(error);
    throw error;
  }
}

async function createOrderInDatabase(
  orderRecord: OrderData,
  logger: StructuredLogger
): Promise<void> {
  const creationTracker = new PerformanceTracker(
    logger,
    "dynamodb-order-creation"
  );

  try {
    logger.info("Starting order creation in DynamoDB", {
      orderId: orderRecord.id,
      leadId: orderRecord.leadId,
      cpf: maskSensitiveData.cpf(orderRecord.cpf),
      email: maskSensitiveData.email(orderRecord.email),
      totalItems: orderRecord.totalItems,
      totalValue: orderRecord.totalValue,
    });

    const putCommand = new PutCommand({
      TableName: ORDER_TABLE_NAME,
      Item: orderRecord,
      ConditionExpression: "attribute_not_exists(orderId)",
    });

    await docClient.send(putCommand);

    logger.info("Order successfully saved to DynamoDB", {
      orderId: orderRecord.id,
      leadId: orderRecord.leadId,
      cpf: maskSensitiveData.cpf(orderRecord.cpf),
      email: maskSensitiveData.email(orderRecord.email),
      totalItems: orderRecord.totalItems,
      totalValue: orderRecord.totalValue,
      status: orderRecord.status,
    });

    creationTracker.finish({
      orderId: orderRecord.id,
      action: "order_created",
      totalItems: orderRecord.totalItems,
      totalValue: orderRecord.totalValue,
      leadId: orderRecord.leadId,
    });
  } catch (error) {
    logger.error("Failed to create order in DynamoDB", error, {
      orderId: orderRecord.id,
      leadId: orderRecord.leadId,
      cpf: maskSensitiveData.cpf(orderRecord.cpf || ""),
      email: maskSensitiveData.email(orderRecord.email || ""),
    });

    creationTracker.finishWithError(error);
    throw error;
  }
}

async function enrichOrderItems(
  items: Pick<OrderItem, "id" | "quantity">[],
  logger: StructuredLogger
): Promise<OrderItem[]> {
  const enrichmentTracker = new PerformanceTracker(
    logger,
    "order-items-enrichment"
  );

  try {
    logger.info("Starting order items enrichment", {
      itemsCount: items.length,
      itemIds: items.map((item) => item.id),
    });

    const enrichedItems: OrderItem[] = [];

    for (const item of items) {
      const product = await getProductById(item.id, logger);

      if (product) {
        const productHasStockControl = product.hasStockControl ?? false;

        let currentStock = 0;
        if (productHasStockControl) {
          currentStock = await calculateCurrentStock(item.id);
          
          if (currentStock < item.quantity) {
            logger.error("Insufficient stock for product", {
              productId: item.id,
              productName: product.name,
              requestedQuantity: item.quantity,
              availableStock: currentStock,
            });

            throw new Error(
              `Insufficient stock for product ${product.name}. Requested: ${item.quantity}, Available: ${currentStock}`
            );
          }
        }

        const enrichedItem: OrderItem = {
          id: item.id,
          quantity: item.quantity,
          productName: product.name,
          unitPrice: product.price,
          totalPrice: product.price * item.quantity,
          hasStockControl: productHasStockControl,
        };

        enrichedItems.push(enrichedItem);

        logger.info("Item enriched successfully", {
          productId: item.id,
          productName: product.name,
          quantity: item.quantity,
          unitPrice: product.price,
          totalPrice: enrichedItem.totalPrice,
          hasStockControl: productHasStockControl,
          currentStock: productHasStockControl ? currentStock : 'N/A',
        });
      } else {
        logger.warn("Product not found, using basic item data", {
          productId: item.id,
          quantity: item.quantity,
        });

        enrichedItems.push({
          id: item.id,
          quantity: item.quantity,
          productName: "Unknown Product",
          unitPrice: 0,
          totalPrice: 0,
          hasStockControl: false,
        });
      }
    }

    enrichmentTracker.finish({
      totalItems: enrichedItems.length,
      totalValue: enrichedItems.reduce(
        (total: number, item: OrderItem) => total + (item.totalPrice || 0),
        0
      ),
    });

    logger.info("Order items enrichment completed", {
      enrichedItemsCount: enrichedItems.length,
      totalValue: enrichedItems.reduce(
        (total: number, item: OrderItem) => total + (item.totalPrice || 0),
        0
      ),
    });

    return enrichedItems;
  } catch (error) {
    logger.error("Error enriching order items", error, {
      itemsCount: items.length,
      itemIds: items.map((item) => item.id),
    });

    enrichmentTracker.finishWithError(error);
    throw error;
  }
}

async function getProductById(
  productId: string,
  logger: StructuredLogger
): Promise<Pick<ProductData, "name" | "price" | "hasStockControl"> | null> {
  const productTracker = new PerformanceTracker(logger, "product-lookup");

  try {
    logger.info("Looking up product", {
      productId: productId,
    });

    const getCommand = new GetCommand({
      TableName: PRODUCT_TABLE_NAME,
      Key: {
        id: productId,
      },
      ProjectionExpression:
        "id, #name, price, category, description, isActive, hasStockControl",
      ExpressionAttributeNames: {
        "#name": "name",
      },
    });

    const result = await docClient.send(getCommand);

    if (result.Item) {
      const product: ProductData = {
        id: result.Item.id as string,
        name: result.Item.name as string,
        price: result.Item.price as number,
        description: result.Item.description as string,
        isActive: result.Item.isActive as boolean,
        hasStockControl: result.Item.hasStockControl as boolean,
      };

      if (!product.isActive) {
        logger.warn("Product found but is inactive", {
          productId: productId,
          productName: product.name,
        });

        productTracker.finish({
          productId: productId,
          found: true,
          active: false,
        });

        return null;
      }

      logger.info("Product found successfully", {
        productId: productId,
        productName: product.name,
        price: product.price,
      });

      productTracker.finish({
        productId: productId,
        found: true,
        active: true,
        price: product.price,
      });

      return product;
    } else {
      logger.warn("Product not found", {
        productId: productId,
      });

      productTracker.finish({
        productId: productId,
        found: false,
      });

      return null;
    }
  } catch (error) {
    logger.error("Error looking up product", error, {
      productId: productId,
    });

    productTracker.finishWithError(error);
    throw error;
  }
}

async function sendStockUpdateMessages(
  items: OrderItem[],
  orderId: string,
  logger: StructuredLogger
): Promise<void> {
  const stockUpdateTracker = new PerformanceTracker(
    logger,
    "stock-update-messages"
  );

  try {
    const itemsWithStockControl = items.filter(
      (item) => item.hasStockControl && item.quantity > 0
    );

    logger.info("Sending stock update messages", {
      totalItemsCount: items.length,
      itemsWithStockControlCount: itemsWithStockControl.length,
      itemsWithStockControl: itemsWithStockControl.map((item) => ({
        productId: item.id,
        quantity: item.quantity,
        productName: item.productName,
      })),
    });

    if (itemsWithStockControl.length === 0) {
      logger.info(
        "No products with stock control found, skipping stock update"
      );
      stockUpdateTracker.finish({
        sentMessages: 0,
        totalQuantityReserved: 0,
        skippedReason: "no_stock_control",
      });
      return;
    }

    const sendPromises = itemsWithStockControl.map(async (item) => {
      const stockMessage: StockUpdateMessage = {
        productId: item.id,
        quantity: item.quantity,
        operation: StockOperationType.DECREASE,
        orderId: orderId,
        reason: "Order sale",
      };

      try {
        logger.info("Sending stock decrease message", {
          productId: item.id,
          productName: item.productName,
          quantityToDecrease: item.quantity,
        });

        const sendCommand = new SendMessageCommand({
          QueueUrl: PRODUCT_STOCK_QUEUE_URL,
          MessageBody: JSON.stringify(stockMessage),
          MessageAttributes: {
            operation: {
              DataType: "String",
              StringValue: stockMessage.operation,
            },
            productId: {
              DataType: "String",
              StringValue: stockMessage.productId,
            },
            orderId: {
              DataType: "String",
              StringValue: orderId,
            },
          },
        });

        await sqsClient.send(sendCommand);

        logger.info("Stock decrease message sent successfully", {
          productId: item.id,
          productName: item.productName,
          quantity: item.quantity,
          operation: stockMessage.operation,
          orderId: orderId,
        });
      } catch (error) {
        logger.error("Failed to send stock decrease message", error, {
          productId: item.id,
          productName: item.productName,
          quantity: item.quantity,
          orderId: orderId,
        });
        throw error;
      }
    });

    await Promise.all(sendPromises);

    stockUpdateTracker.finish({
      sentMessages: itemsWithStockControl.length,
      totalQuantityReserved: itemsWithStockControl.reduce(
        (total, item) => total + item.quantity,
        0
      ),
    });

    logger.info("All stock update messages sent successfully", {
      totalItemsCount: items.length,
      sentMessagesCount: itemsWithStockControl.length,
      totalQuantityReserved: itemsWithStockControl.reduce(
        (total, item) => total + item.quantity,
        0
      ),
    });
  } catch (error) {
    logger.error("Error sending stock update messages", error, {
      itemsCount: items.length,
      orderId: orderId,
      items: items.map((item) => ({
        productId: item.id,
        quantity: item.quantity,
        productName: item.productName,
      })),
    });

    stockUpdateTracker.finishWithError(error);
    throw error;
  }
}

async function sendTransactionMessage(
  orderId: string,
  orderTotalValue: number,
  paymentData: PaymentData,
  addressData: AddressData,
  customerData: CustomerData,
  logger: StructuredLogger
): Promise<void> {
  const transactionTracker = new PerformanceTracker(
    logger,
    "transaction-message-send"
  );

  try {
    logger.info("Sending transaction processing message", {
      orderId: orderId,
      cardLastFour: paymentData.cardNumber?.slice(-4),
      name: maskSensitiveData.name(customerData.name),
      addressCity: addressData.city,
      addressState: addressData.state,
    });

    const transactionMessage: TransactionMessage = {
      orderId: orderId,
      orderTotalValue,
      paymentData: paymentData,
      addressData: addressData,
      customerData: customerData,
    };

    const sendCommand = new SendMessageCommand({
      QueueUrl: PROCESS_TRANSACTION_QUEUE_URL,
      MessageBody: JSON.stringify(transactionMessage),
      MessageAttributes: {
        orderId: {
          DataType: "String",
          StringValue: orderId,
        },
        amount: {
          DataType: "Number",
          StringValue: orderTotalValue.toString(),
        },
        email: {
          DataType: "String",
          StringValue: customerData.email,
        },
      },
    });

    await sqsClient.send(sendCommand);

    transactionTracker.finish({
      orderId: orderId,
      amount: orderTotalValue,
      queueUrl: PROCESS_TRANSACTION_QUEUE_URL,
    });

    logger.info("Transaction processing message sent successfully", {
      orderId: orderId,
      amount: orderTotalValue,
      cardLastFour: paymentData.cardNumber?.slice(-4),
      name: maskSensitiveData.name(customerData.name),
    });
  } catch (error) {
    logger.error("Failed to send transaction processing message", error, {
      orderId: orderId,
      amount: orderTotalValue,
      name: maskSensitiveData.name(customerData.name),
    });

    transactionTracker.finishWithError(error);
  }
}
