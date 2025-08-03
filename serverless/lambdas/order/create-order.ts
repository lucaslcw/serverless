import { SQSEvent } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import {
  DynamoDBDocumentClient,
  QueryCommand,
  PutCommand,
  GetCommand,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";
import {
  createLogger,
  generateCorrelationId,
  maskSensitiveData,
  PerformanceTracker,
} from "../../shared/logger";

const dynamoClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const sqsClient = new SQSClient({ region: process.env.AWS_REGION });
const docClient = DynamoDBDocumentClient.from(dynamoClient);

const LEAD_TABLE_NAME = process.env.LEAD_COLLECTION_TABLE!;
const ORDER_TABLE_NAME = process.env.ORDER_COLLECTION_TABLE!;
const PRODUCT_TABLE_NAME = process.env.PRODUCT_COLLECTION_TABLE!;
const PRODUCT_STOCK_QUEUE_URL = process.env.PRODUCT_STOCK_QUEUE_URL!;
const PROCESS_TRANSACTION_QUEUE_URL = process.env.PROCESS_TRANSACTION_QUEUE_URL!;

enum OrderStatus {
  PENDING = "PENDING",
  PROCESSED = "PROCESSED",
}

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

interface TransactionMessage {
  orderId: string;
  paymentData: any;
  addressData: any;
  customerInfo: {
    name: string;
    email: string;
    cpf: string;
  };
}

interface OrderItem {
  id: string;
  quantity: number;
  productName?: string;
  unitPrice?: number;
  totalPrice?: number;
  hasStockControl?: boolean;
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

interface OrderData {
  orderId: string;
  cpf: string;
  email: string;
  name: string;
  items: OrderItem[];
  paymentData?: any;
  addressData?: any;
  timestamp: string;
  status: string;
}

interface ExistingLead {
  id: string;
  leadId: string;
  customerCpf: string;
  customerEmail: string;
  customerName: string;
  createdAt: string;
  updatedAt: string;
}

interface OrderRecord {
  orderId: string;
  leadId: string;
  customerCpf: string;
  customerEmail: string;
  customerName: string;
  items: OrderItem[];
  totalItems: number;
  totalValue: number;
  status: OrderStatus;
  source: string;
  createdAt: string;
  updatedAt: string;
}

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
      const message: OrderData = JSON.parse(snsMessage.Message);

      const orderLogger = recordLogger.withContext({
        orderId: message.orderId,
      });

      orderLogger.info("Order data parsed successfully", {
        customerCpf: maskSensitiveData.cpf(message.cpf || ""),
        customerEmail: maskSensitiveData.email(message.email || ""),
        customerName: maskSensitiveData.name(message.name || ""),
        itemsCount: message.items ? message.items.length : 0,
        orderStatus: message.status,
        orderTimestamp: message.timestamp,
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

      const stockUpdatedItems: OrderItem[] = [];

      try {
        await updateProductsStock(
          enrichedItems,
          orderLogger,
          stockUpdatedItems
        );

        const leadId = await findOrCreateLead(
          message.email,
          message.cpf,
          message.name,
          message.orderId,
          orderLogger
        );

        const orderRecord: OrderRecord = {
          orderId: message.orderId,
          leadId: leadId,
          customerCpf: message.cpf,
          customerEmail: message.email,
          customerName: message.name,
          items: enrichedItems,
          totalItems: enrichedItems.reduce(
            (total: number, item: OrderItem) => total + item.quantity,
            0
          ),
          totalValue: totalValue,
          status: OrderStatus.PENDING,
          source: "order-processing-service",
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
        };

        await createOrderInDatabase(orderRecord, orderLogger);

        if (message.paymentData && message.addressData) {
          await sendTransactionMessage(
            message.orderId,
            message.paymentData,
            message.addressData,
            {
              name: message.name,
              email: message.email,
              cpf: message.cpf,
            },
            orderLogger
          );
        }

        processingTracker.finish({
          newStatus: orderRecord.status,
          totalItems: orderRecord.totalItems,
          totalValue: orderRecord.totalValue,
          leadId: leadId,
          leadAction: "associated",
        });

        orderLogger.info("Order created successfully", {
          newStatus: orderRecord.status,
          totalItems: orderRecord.totalItems,
          totalValue: orderRecord.totalValue,
          leadId: leadId,
          leadRequired: true,
        });
      } catch (orderError) {
        if (stockUpdatedItems.length > 0) {
          orderLogger.warn(
            "Order creation failed, sending stock rollback messages",
            {
              stockUpdatedItemsCount: stockUpdatedItems.length,
              orderId: message.orderId,
            }
          );

          await sendStockRollbackMessages(
            stockUpdatedItems,
            message.orderId,
            orderLogger
          );
        }

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
  logger: any
): Promise<string> {
  const leadTracker = new PerformanceTracker(logger, "find-or-create-lead");

  try {
    logger.info("Searching for existing lead or creating new one", {
      customerCpf: maskSensitiveData.cpf(cpf),
      customerEmail: maskSensitiveData.email(email),
      orderReference: orderReference,
    });

    const existingLead = await findExistingLead(email, cpf, logger);

    if (existingLead) {
      logger.info("Found existing lead, using existing leadId", {
        existingLeadId: existingLead.leadId,
        customerCpf: maskSensitiveData.cpf(cpf),
        customerEmail: maskSensitiveData.email(email),
      });

      leadTracker.finish({
        action: "found_existing_lead",
        leadId: existingLead.leadId,
      });

      return existingLead.leadId;
    }

    const newLeadId = `lead-${Date.now()}-${Math.random()
      .toString(36)
      .substring(2, 15)}`;

    const newLeadData = {
      id: newLeadId,
      leadId: newLeadId,
      customerCpf: cpf,
      customerEmail: email,
      customerName: name,
      orderReference: orderReference,
      totalItems: 0,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      source: "order-processing-service",
      status: "created_from_order",
    };

    await createLeadInDatabase(newLeadData, logger);

    logger.info("Created new lead for order", {
      newLeadId: newLeadId,
      customerCpf: maskSensitiveData.cpf(cpf),
      customerEmail: maskSensitiveData.email(email),
      orderReference: orderReference,
    });

    leadTracker.finish({
      action: "created_new_lead",
      leadId: newLeadId,
    });

    return newLeadId;
  } catch (error) {
    logger.error("Error finding or creating lead", error, {
      customerCpf: maskSensitiveData.cpf(cpf || ""),
      customerEmail: maskSensitiveData.email(email || ""),
      orderReference: orderReference,
    });

    leadTracker.finishWithError(error);
    throw error;
  }
}
async function findExistingLead(
  email: string,
  cpf: string,
  logger: any
): Promise<ExistingLead | null> {
  const searchTracker = new PerformanceTracker(logger, "lead-search-query");

  try {
    logger.info("Searching for existing lead", {
      customerCpf: maskSensitiveData.cpf(cpf),
      customerEmail: maskSensitiveData.email(email),
    });

    const emailQueryCommand = new QueryCommand({
      TableName: LEAD_TABLE_NAME,
      IndexName: "email-index",
      KeyConditionExpression: "customerEmail = :email",
      ExpressionAttributeValues: {
        ":email": email,
      },
      ProjectionExpression:
        "id, leadId, customerCpf, customerEmail, customerName, createdAt, updatedAt",
    });

    const emailResult = await docClient.send(emailQueryCommand);

    if (emailResult.Items && emailResult.Items.length > 0) {
      const matchingLead = emailResult.Items.find(
        (item) => item.customerCpf === cpf
      );

      if (matchingLead) {
        logger.info("Found existing lead with matching email and CPF", {
          existingLeadId: matchingLead.id,
          leadId: matchingLead.leadId,
          customerCpf: maskSensitiveData.cpf(cpf),
          customerEmail: maskSensitiveData.email(email),
        });

        searchTracker.finish({
          action: "found_existing_lead",
          leadId: matchingLead.leadId,
        });

        return {
          id: matchingLead.id as string,
          leadId: matchingLead.leadId as string,
          customerCpf: matchingLead.customerCpf as string,
          customerEmail: matchingLead.customerEmail as string,
          customerName: matchingLead.customerName as string,
          createdAt: matchingLead.createdAt as string,
          updatedAt: matchingLead.updatedAt as string,
        };
      }
    }

    logger.info("No existing lead found for email and CPF combination", {
      customerCpf: maskSensitiveData.cpf(cpf),
      customerEmail: maskSensitiveData.email(email),
      emailLeadsCount: emailResult.Items?.length || 0,
    });

    searchTracker.finish({
      action: "no_lead_found",
      emailLeadsCount: emailResult.Items?.length || 0,
    });

    return null;
  } catch (error) {
    logger.error("Error searching for existing lead", error, {
      customerCpf: maskSensitiveData.cpf(cpf || ""),
      customerEmail: maskSensitiveData.email(email || ""),
    });

    searchTracker.finishWithError(error);
    throw error;
  }
}

async function createLeadInDatabase(leadData: any, logger: any): Promise<void> {
  const creationTracker = new PerformanceTracker(
    logger,
    "dynamodb-lead-creation"
  );

  try {
    logger.info("Creating new lead in DynamoDB", {
      leadId: leadData.leadId,
      customerCpf: maskSensitiveData.cpf(leadData.customerCpf),
      customerEmail: maskSensitiveData.email(leadData.customerEmail),
    });

    const putCommand = new PutCommand({
      TableName: LEAD_TABLE_NAME,
      Item: leadData,
      ConditionExpression: "attribute_not_exists(id)",
    });

    await docClient.send(putCommand);

    logger.info("Lead successfully created in DynamoDB", {
      leadId: leadData.leadId,
      customerCpf: maskSensitiveData.cpf(leadData.customerCpf),
      customerEmail: maskSensitiveData.email(leadData.customerEmail),
      status: leadData.status,
    });

    creationTracker.finish({
      leadId: leadData.leadId,
      action: "lead_created",
    });
  } catch (error) {
    logger.error("Failed to create lead in DynamoDB", error, {
      leadId: leadData.leadId,
      customerCpf: maskSensitiveData.cpf(leadData.customerCpf || ""),
      customerEmail: maskSensitiveData.email(leadData.customerEmail || ""),
    });

    creationTracker.finishWithError(error);
    throw error;
  }
}

async function createOrderInDatabase(
  orderRecord: OrderRecord,
  logger: any
): Promise<void> {
  const creationTracker = new PerformanceTracker(
    logger,
    "dynamodb-order-creation"
  );

  try {
    logger.info("Starting order creation in DynamoDB", {
      orderId: orderRecord.orderId,
      leadId: orderRecord.leadId,
      customerCpf: maskSensitiveData.cpf(orderRecord.customerCpf),
      customerEmail: maskSensitiveData.email(orderRecord.customerEmail),
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
      orderId: orderRecord.orderId,
      leadId: orderRecord.leadId,
      customerCpf: maskSensitiveData.cpf(orderRecord.customerCpf),
      customerEmail: maskSensitiveData.email(orderRecord.customerEmail),
      totalItems: orderRecord.totalItems,
      totalValue: orderRecord.totalValue,
      status: orderRecord.status,
    });

    creationTracker.finish({
      orderId: orderRecord.orderId,
      action: "order_created",
      totalItems: orderRecord.totalItems,
      totalValue: orderRecord.totalValue,
      leadId: orderRecord.leadId,
    });
  } catch (error) {
    logger.error("Failed to create order in DynamoDB", error, {
      orderId: orderRecord.orderId,
      leadId: orderRecord.leadId,
      customerCpf: maskSensitiveData.cpf(orderRecord.customerCpf || ""),
      customerEmail: maskSensitiveData.email(orderRecord.customerEmail || ""),
    });

    creationTracker.finishWithError(error);
    throw error;
  }
}

async function enrichOrderItems(
  items: OrderItem[],
  logger: any
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
        if (product.quantityInStock !== undefined) {
          if (product.quantityInStock < item.quantity) {
            logger.error("Insufficient stock for product", {
              productId: item.id,
              productName: product.name,
              requestedQuantity: item.quantity,
              availableStock: product.quantityInStock,
            });

            throw new Error(
              `Insufficient stock for product ${product.name}. Requested: ${item.quantity}, Available: ${product.quantityInStock}`
            );
          }
        }

        const enrichedItem: OrderItem = {
          id: item.id,
          quantity: item.quantity,
          productName: product.name,
          unitPrice: product.price,
          totalPrice: product.price * item.quantity,
          hasStockControl: product.quantityInStock !== undefined,
        };

        enrichedItems.push(enrichedItem);

        logger.info("Item enriched successfully", {
          productId: item.id,
          productName: product.name,
          quantity: item.quantity,
          unitPrice: product.price,
          totalPrice: enrichedItem.totalPrice,
          hasStockControl: product.quantityInStock !== undefined,
          availableStock: product.quantityInStock,
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
  logger: any
): Promise<Product | null> {
  const productTracker = new PerformanceTracker(logger, "product-lookup");

  try {
    logger.info("Looking up product", {
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
        category: product.category,
        hasStockControl: product.quantityInStock !== undefined,
        quantityInStock: product.quantityInStock,
      });

      productTracker.finish({
        productId: productId,
        found: true,
        active: true,
        price: product.price,
        hasStockControl: product.quantityInStock !== undefined,
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

async function updateProductsStock(
  items: OrderItem[],
  logger: any,
  stockUpdatedItems?: OrderItem[]
): Promise<void> {
  const stockUpdateTracker = new PerformanceTracker(
    logger,
    "products-stock-update"
  );

  try {
    const itemsWithStockControl = items.filter(
      (item) => item.hasStockControl && item.quantity > 0
    );

    logger.info("Starting products stock update", {
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
        updatedProducts: 0,
        totalQuantitySubtracted: 0,
        skippedReason: "no_stock_control",
      });
      return;
    }

    const updatePromises = itemsWithStockControl.map(async (item) => {
      try {
        logger.info("Updating stock for product", {
          productId: item.id,
          productName: item.productName,
          quantityToSubtract: item.quantity,
        });

        const updateCommand = new UpdateCommand({
          TableName: PRODUCT_TABLE_NAME,
          Key: {
            productId: item.id,
          },
          UpdateExpression:
            "SET quantityInStock = quantityInStock - :quantity, updatedAt = :updatedAt",
          ConditionExpression:
            "quantityInStock >= :quantity AND isActive = :isActive",
          ExpressionAttributeValues: {
            ":quantity": item.quantity,
            ":isActive": true,
            ":updatedAt": new Date().toISOString(),
          },
          ReturnValues: "ALL_NEW",
        });

        const result = await docClient.send(updateCommand);

        if (stockUpdatedItems) {
          stockUpdatedItems.push(item);
        }

        logger.info("Stock updated successfully", {
          productId: item.id,
          productName: item.productName,
          previousStock:
            (result.Attributes?.quantityInStock as number) + item.quantity,
          newStock: result.Attributes?.quantityInStock as number,
          quantitySubtracted: item.quantity,
        });
      } catch (error: any) {
        logger.error("Failed to update stock for product", error, {
          productId: item.id,
          productName: item.productName,
          quantityToSubtract: item.quantity,
          errorCode: error.name,
        });

        if (error.name === "ConditionalCheckFailedException") {
          throw new Error(
            `Stock update failed for product ${item.productName}. Insufficient stock or product is inactive.`
          );
        }

        throw error;
      }
    });

    await Promise.all(updatePromises);

    stockUpdateTracker.finish({
      updatedProducts: itemsWithStockControl.length,
      totalQuantitySubtracted: itemsWithStockControl.reduce(
        (total, item) => total + item.quantity,
        0
      ),
    });

    logger.info("All products stock updated successfully", {
      totalItemsCount: items.length,
      updatedProductsCount: itemsWithStockControl.length,
      totalQuantitySubtracted: itemsWithStockControl.reduce(
        (total, item) => total + item.quantity,
        0
      ),
    });
  } catch (error) {
    logger.error("Error updating products stock", error, {
      itemsCount: items.length,
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

async function sendStockRollbackMessages(
  stockUpdatedItems: OrderItem[],
  orderId: string,
  logger: any
): Promise<void> {
  const rollbackTracker = new PerformanceTracker(
    logger,
    "stock-rollback-messages"
  );

  try {
    logger.info("Sending stock rollback messages", {
      itemsCount: stockUpdatedItems.length,
      orderId: orderId,
      items: stockUpdatedItems.map((item) => ({
        productId: item.id,
        quantity: item.quantity,
        productName: item.productName,
      })),
    });

    const sendPromises = stockUpdatedItems.map(async (item) => {
      const stockMessage: StockUpdateMessage = {
        productId: item.id,
        quantity: item.quantity,
        operation: StockOperationType.INCREASE,
        orderId: orderId,
        reason: "Order creation failed - rolling back stock reduction",
      };

      try {
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

        logger.info("Stock rollback message sent successfully", {
          productId: item.id,
          productName: item.productName,
          quantity: item.quantity,
          operation: stockMessage.operation,
          orderId: orderId,
        });
      } catch (error) {
        logger.error("Failed to send stock rollback message", error, {
          productId: item.id,
          productName: item.productName,
          quantity: item.quantity,
          orderId: orderId,
        });
      }
    });

    await Promise.all(sendPromises);

    rollbackTracker.finish({
      rollbackMessagesCount: stockUpdatedItems.length,
      orderId: orderId,
    });

    logger.info("All stock rollback messages sent", {
      totalMessages: stockUpdatedItems.length,
      orderId: orderId,
    });
  } catch (error) {
    logger.error("Error sending stock rollback messages", error, {
      itemsCount: stockUpdatedItems.length,
      orderId: orderId,
    });

    rollbackTracker.finishWithError(error);
  }
}

async function sendTransactionMessage(
  orderId: string,
  paymentData: any,
  addressData: any,
  customerInfo: { name: string; email: string; cpf: string },
  logger: any
): Promise<void> {
  const transactionTracker = new PerformanceTracker(
    logger,
    "transaction-message-send"
  );

  try {
    logger.info("Sending transaction processing message", {
      orderId: orderId,
      amount: paymentData.amount,
      cardLastFour: paymentData.cardNumber?.slice(-4),
      customerName: maskSensitiveData.name(customerInfo.name),
      addressCity: addressData.city,
      addressState: addressData.state,
    });

    const transactionMessage: TransactionMessage = {
      orderId: orderId,
      paymentData: paymentData,
      addressData: addressData,
      customerInfo: customerInfo,
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
          StringValue: paymentData.amount.toString(),
        },
        customerEmail: {
          DataType: "String",
          StringValue: customerInfo.email,
        },
      },
    });

    await sqsClient.send(sendCommand);

    transactionTracker.finish({
      orderId: orderId,
      amount: paymentData.amount,
      queueUrl: PROCESS_TRANSACTION_QUEUE_URL,
    });

    logger.info("Transaction processing message sent successfully", {
      orderId: orderId,
      amount: paymentData.amount,
      cardLastFour: paymentData.cardNumber?.slice(-4),
      customerName: maskSensitiveData.name(customerInfo.name),
    });

  } catch (error) {
    logger.error("Failed to send transaction processing message", error, {
      orderId: orderId,
      amount: paymentData.amount,
      customerName: maskSensitiveData.name(customerInfo.name),
    });

    transactionTracker.finishWithError(error);
  }
}
