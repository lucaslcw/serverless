import { SQSEvent, SQSRecord } from "aws-lambda";
import { handler } from "../lambdas/order/create-order";

jest.mock("@aws-sdk/client-dynamodb");
jest.mock("@aws-sdk/lib-dynamodb", () => {
  const mockSend = jest.fn();
  return {
    DynamoDBDocumentClient: {
      from: jest.fn(() => ({
        send: mockSend,
      })),
    },
    QueryCommand: jest.fn().mockImplementation((input) => ({
      input,
      constructor: { name: "QueryCommand" },
    })),
    PutCommand: jest.fn().mockImplementation((input) => ({
      input,
      constructor: { name: "PutCommand" },
    })),
    GetCommand: jest.fn().mockImplementation((input) => ({
      input,
      constructor: { name: "GetCommand" },
    })),
    UpdateCommand: jest.fn().mockImplementation((input) => ({
      input,
      constructor: { name: "UpdateCommand" },
    })),
  };
});

jest.mock("@aws-sdk/client-sqs", () => {
  const mockSend = jest.fn();
  return {
    SQSClient: jest.fn(() => ({
      send: mockSend,
    })),
    SendMessageCommand: jest.fn().mockImplementation((input) => ({
      input,
      constructor: { name: "SendMessageCommand" },
    })),
  };
});

jest.mock("../shared/logger", () => ({
  createLogger: jest.fn(),
  generateCorrelationId: jest.fn(),
  maskSensitiveData: {
    cpf: jest.fn(),
    email: jest.fn(),
    name: jest.fn(),
  },
  PerformanceTracker: jest.fn(),
}));

import {
  createLogger,
  generateCorrelationId,
  maskSensitiveData,
  PerformanceTracker,
} from "../shared/logger";

import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { SQSClient } from "@aws-sdk/client-sqs";
import { MessageData } from "../lambdas/order/initialize-order";

describe("create-order handler", () => {
  let mockEvent: SQSEvent;
  let mockLogger: any;
  let mockTracker: any;
  let mockSend: jest.Mock;
  let mockSqsSend: jest.Mock;

  const messageData: MessageData = {
    orderId: "order-123456789-abc123",
    customerData: {
      cpf: "12345678901",
      email: "test@example.com",
      name: "João Silva",
    },
    paymentData: {
      cvv: "123",
      expiryMonth: "12",
      expiryYear: "2025",
      cardHolderName: "Lucas",
      cardNumber: "4111111111111111",
    },
    addressData: {
      street: "Av. Brasil",
      number: "1000",
      city: "São Paulo",
      state: "SP",
      neighborhood: "Centro",
      complement: "Apto 101",
      country: "Brasil",
      zipCode: "12345-678",
    },
    items: [
      { id: "item1", quantity: 2 },
      { id: "item2", quantity: 1 },
    ],
  };

  beforeEach(() => {
    jest.clearAllMocks();

    const mockDocClientInstance = (DynamoDBDocumentClient.from as jest.Mock)();
    mockSend = mockDocClientInstance.send as jest.Mock;
    mockSend.mockClear();

    const mockSqsClientInstance = new (SQSClient as jest.Mock)();
    mockSqsSend = mockSqsClientInstance.send as jest.Mock;
    mockSqsSend.mockClear();

    process.env.LEAD_COLLECTION_TABLE = "test-lead-collection";
    process.env.ORDER_COLLECTION_TABLE = "test-order-collection";
    process.env.PRODUCT_COLLECTION_TABLE = "test-product-collection";
    process.env.PRODUCT_STOCK_QUEUE_URL = "test-product-stock-queue";
    process.env.PROCESS_TRANSACTION_QUEUE_URL = "test-process-transaction-queue";

    mockLogger = {
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
      withContext: jest.fn().mockReturnThis(),
    };
    (createLogger as jest.Mock).mockReturnValue(mockLogger);
    (generateCorrelationId as jest.Mock).mockReturnValue(
      "order-test-correlation-id"
    );

    mockTracker = {
      finish: jest.fn(),
      finishWithError: jest.fn(),
    };
    (PerformanceTracker as jest.Mock).mockReturnValue(mockTracker);

    (maskSensitiveData.cpf as jest.Mock).mockReturnValue("123***901");
    (maskSensitiveData.email as jest.Mock).mockReturnValue(
      "test***@example.com"
    );
    (maskSensitiveData.name as jest.Mock).mockReturnValue("Jo***");

    mockSend.mockImplementation((command: any) => {
      if (
        command.constructor.name === "QueryCommand" ||
        command.input?.IndexName === "email-index"
      ) {
        return Promise.resolve({ Items: [] });
      }

      if (
        command.constructor.name === "GetCommand" ||
        command.input?.Key?.id
      ) {
        const productId = command.input?.Key?.id;
        if (productId === "item1") {
          return Promise.resolve({
            Item: {
              id: "item1",
              name: "Produto A",
              price: 29.99,
              category: "electronics",
              description: "Produto eletrônico A",
              isActive: true,
              quantityInStock: 100,
            },
          });
        } else if (productId === "item2") {
          return Promise.resolve({
            Item: {
              id: "item2",
              name: "Produto B",
              price: 15.5,
              category: "accessories",
              description: "Acessório B",
              isActive: true,
              quantityInStock: 50,
            },
          });
        } else if (productId === "item3") {
          return Promise.resolve({
            Item: {
              id: "item3",
              name: "Produto C",
              price: 45.0,
              category: "tools",
              description: "Ferramenta C",
              isActive: true,
              quantityInStock: 25,
            },
          });
        } else if (productId === "digital-item") {
          return Promise.resolve({
            Item: {
              id: "digital-item",
              name: "Produto Digital",
              price: 99.99,
              category: "digital",
              description: "Produto digital sem controle de estoque",
              isActive: true,
            },
          });
        }
        return Promise.resolve({ Item: null });
      }

      if (command.constructor.name === "UpdateCommand") {
        const productId = command.input?.Key?.id;
        const quantityToSubtract =
          command.input?.ExpressionAttributeValues?.[":quantity"];

        let newStock = 0;
        if (productId === "item1") {
          newStock = 100 - quantityToSubtract;
        } else if (productId === "item2") {
          newStock = 50 - quantityToSubtract;
        } else if (productId === "item3") {
          newStock = 25 - quantityToSubtract;
        }

        return Promise.resolve({
          Attributes: {
            id: productId,
            quantityInStock: newStock,
            updatedAt: new Date().toISOString(),
          },
        });
      }

      return Promise.resolve({});
    });

    // Mock SQS send for transaction and stock messages
    mockSqsSend.mockResolvedValue({
      MessageId: "mock-message-id",
      MD5OfBody: "mock-md5",
    });

    mockEvent = {
      Records: [
        {
          messageId: "test-message-id-1",
          receiptHandle: "test-receipt-handle-1",
          body: JSON.stringify({
            Type: "Notification",
            MessageId: "sns-message-id-1",
            TopicArn:
              "arn:aws:sns:us-east-1:123456789012:initialize-order-topic",
            Subject: "New Order Request",
            Message: JSON.stringify(messageData),
            Timestamp: "2025-08-03T10:30:00.000Z",
          }),
          attributes: {
            ApproximateReceiveCount: "1",
            SentTimestamp: "1691055000000",
            SenderId: "AIDAIENQZJOLO23YVJ4VO",
            ApproximateFirstReceiveTimestamp: "1691055000000",
          },
          messageAttributes: {},
          md5OfBody: "test-md5-hash",
          eventSource: "aws:sqs",
          eventSourceARN:
            "arn:aws:sqs:us-east-1:123456789012:create-order-queue",
          awsRegion: "us-east-1",
        } as SQSRecord,
      ],
    };
  });

  describe("Successful Processing", () => {
    test("should process single order successfully", async () => {
      await handler(mockEvent);

      expect(createLogger).toHaveBeenCalledWith({
        processId: "order-test-correlation-id",
        functionName: "create-order",
      });

      expect(mockLogger.info).toHaveBeenCalledWith(
        "Processing SQS batch started",
        {
          recordsCount: 1,
        }
      );

      expect(mockLogger.info).toHaveBeenCalledWith(
        "SQS batch processing completed",
        {
          totalRecords: 1,
        }
      );

      expect(PerformanceTracker).toHaveBeenCalledWith(
        mockLogger,
        "create-order-batch"
      );
      expect(mockTracker.finish).toHaveBeenCalledWith({
        totalRecords: 1,
        status: "completed",
      });
    });

    test("should process order data and mask sensitive information", async () => {
      await handler(mockEvent);

      expect(maskSensitiveData.cpf).toHaveBeenCalledWith("12345678901");
      expect(maskSensitiveData.email).toHaveBeenCalledWith("test@example.com");
      expect(maskSensitiveData.name).toHaveBeenCalledWith("João Silva");

      expect(mockLogger.info).toHaveBeenCalledWith(
        "Order data parsed successfully",
        {
          cpf: "123***901",
          email: "test***@example.com",
          name: "Jo***",
          itemsCount: 2,
        }
      );
    });

    test("should create order with correct status and calculate total items", async () => {
      await handler(mockEvent);

      const orderCreatedCall = mockLogger.info.mock.calls.find(
        (call: any) => call[0] === "Order created successfully"
      );

      expect(orderCreatedCall).toBeDefined();
      expect(orderCreatedCall[1]).toMatchObject({
        newStatus: "PENDING",
        totalItems: 3,
        leadId: expect.stringMatching(/^lead-\d+-[a-z0-9]+$/),
      });

      expect(orderCreatedCall[1].totalValue).toBeCloseTo(75.48, 2);

      expect(mockSend).toHaveBeenCalledTimes(7);
    });

    test("should search for existing lead and create order", async () => {
      await handler(mockEvent);

      expect(mockSend).toHaveBeenCalledTimes(7);

      expect(mockLogger.info).toHaveBeenCalledWith(
        "SQS batch processing completed",
        {
          totalRecords: 1,
        }
      );

      expect(mockLogger.info).toHaveBeenCalledWith(
        "Created new lead for order",
        {
          newLeadId: expect.stringMatching(/^lead-\d+-[a-z0-9]+$/),
          cpf: "123***901",
          email: "test***@example.com",
          orderReference: "order-123456789-abc123",
        }
      );
    });

    test("should process multiple records in batch", async () => {
      const messageData: MessageData = {
        orderId: "order-987654321-def456",
        customerData: {
          cpf: "98765432100",
          email: "another@example.com",
          name: "Maria Santos",
        },
        paymentData: {
          cardNumber: "4111111111111111",
          expiryYear: "2025",
          expiryMonth: "12",
          cardHolderName: "Maria Santos",
          cvv: "123",
        },
        addressData: {
          street: "Av. Brasil",
          number: "1000",
          complement: "Apto 101",
          city: "São Paulo",
          state: "SP",
          zipCode: "12345-678",
          country: "Brasil",
          neighborhood: "Centro",
        },
        items: [{ id: "item3", quantity: 5 }],
      };
      const secondRecord = {
        ...mockEvent.Records[0],
        messageId: "test-message-id-2",
        receiptHandle: "test-receipt-handle-2",
        body: JSON.stringify({
          Type: "Notification",
          MessageId: "sns-message-id-2",
          TopicArn: "arn:aws:sns:us-east-1:123456789012:initialize-order-topic",
          Subject: "New Order Request",
          Message: JSON.stringify(messageData),
          Timestamp: "2025-08-03T11:00:00.000Z",
        }),
      } as SQSRecord;

      mockEvent.Records.push(secondRecord);

      await handler(mockEvent);

      expect(mockLogger.info).toHaveBeenCalledWith(
        "Processing SQS batch started",
        {
          recordsCount: 2,
        }
      );

      expect(mockLogger.info).toHaveBeenCalledWith(
        "SQS batch processing completed",
        {
          totalRecords: 2,
        }
      );

      expect(mockLogger.info).toHaveBeenCalledWith(
        "Order created successfully",
        {
          newStatus: "PENDING",
          totalItems: 5,
          totalValue: 225.0,
          leadId: expect.stringMatching(/^lead-\d+-[a-z0-9]+$/),
        }
      );
    });

    test("should send transaction processing message with correct data", async () => {
      await handler(mockEvent);

      // Verify SQS send was called for transaction message
      expect(mockSqsSend).toHaveBeenCalledWith(
        expect.objectContaining({
          constructor: { name: "SendMessageCommand" },
          input: expect.objectContaining({
            MessageBody: expect.stringContaining(messageData.orderId),
            MessageAttributes: expect.objectContaining({
              orderId: {
                DataType: "String",
                StringValue: messageData.orderId,
              },
              amount: {
                DataType: "Number",
                StringValue: expect.any(String),
              },
              email: {
                DataType: "String", 
                StringValue: messageData.customerData.email,
              },
            }),
          }),
        })
      );

      // Verify the message body contains the expected structure
      const sendCalls = mockSqsSend.mock.calls.filter((call: any) => 
        call[0].input?.MessageAttributes?.orderId?.StringValue === messageData.orderId
      );
      
      expect(sendCalls).toHaveLength(1);
      
      const messageBody = JSON.parse(sendCalls[0][0].input.MessageBody);
      expect(messageBody).toMatchObject({
        orderId: messageData.orderId,
        orderTotalValue: expect.any(Number),
        paymentData: messageData.paymentData,
        addressData: messageData.addressData,
        customerData: messageData.customerData,
      });

      // Verify the total value is calculated correctly
      expect(messageBody.orderTotalValue).toBeCloseTo(75.48, 2);
    });

    test("should enrich order items with product information and calculate total value", async () => {
      await handler(mockEvent);

      const itemEnrichedCalls = mockLogger.info.mock.calls.filter(
        (call: any) => call[0] === "Item enriched successfully"
      );

      expect(itemEnrichedCalls).toHaveLength(2);
      expect(itemEnrichedCalls[0][1]).toMatchObject({
        productId: "item1",
        productName: "Produto A",
        quantity: 2,
        unitPrice: 29.99,
        totalPrice: 59.98,
      });

      expect(itemEnrichedCalls[1][1]).toMatchObject({
        productId: "item2",
        productName: "Produto B",
        quantity: 1,
        unitPrice: 15.5,
        totalPrice: 15.5,
      });

      const enrichmentCompletedCall = mockLogger.info.mock.calls.find(
        (call: any) => call[0] === "Order items enrichment completed"
      );

      expect(enrichmentCompletedCall).toBeDefined();
      expect(enrichmentCompletedCall[1]).toMatchObject({
        enrichedItemsCount: 2,
      });

      expect(enrichmentCompletedCall[1].totalValue).toBeCloseTo(75.48, 2);
    });
  });

  describe("Error Handling", () => {
    test("should handle invalid JSON in SNS message", async () => {
      mockEvent.Records[0].body = "invalid-json";

      await expect(handler(mockEvent)).rejects.toThrow();

      expect(mockLogger.error).toHaveBeenCalledWith(
        "Error processing order record",
        expect.any(Error),
        {
          sqsMessagePreview: "invalid-json",
        }
      );

      expect(mockTracker.finishWithError).toHaveBeenCalledWith(
        expect.any(Error)
      );
    });

    test("should handle invalid JSON in nested Message field", async () => {
      mockEvent.Records[0].body = JSON.stringify({
        Type: "Notification",
        MessageId: "sns-message-id-1",
        TopicArn: "arn:aws:sns:us-east-1:123456789012:initialize-order-topic",
        Subject: "New Order Request",
        Message: "invalid-nested-json",
        Timestamp: "2025-08-03T10:30:00.000Z",
      });

      await expect(handler(mockEvent)).rejects.toThrow();

      expect(mockLogger.error).toHaveBeenCalledWith(
        "Error processing order record",
        expect.any(Error),
        expect.objectContaining({
          sqsMessagePreview: expect.stringContaining("invalid-nested-json"),
        })
      );
    });

    test("should handle missing order data fields gracefully", async () => {
      const incompleteOrderData = {
        orderId: "order-incomplete-123",
      };

      mockEvent.Records[0].body = JSON.stringify({
        Type: "Notification",
        MessageId: "sns-message-id-1",
        TopicArn: "arn:aws:sns:us-east-1:123456789012:initialize-order-topic",
        Subject: "New Order Request",
        Message: JSON.stringify(incompleteOrderData),
        Timestamp: "2025-08-03T10:30:00.000Z",
      });

      await handler(mockEvent);

      expect(maskSensitiveData.cpf).toHaveBeenCalledWith("");
      expect(maskSensitiveData.email).toHaveBeenCalledWith("");
      expect(maskSensitiveData.name).toHaveBeenCalledWith("");

      expect(mockLogger.info).toHaveBeenCalledWith(
        "Order data parsed successfully",
        {
          cpf: "123***901",
          email: "test***@example.com",
          name: "Jo***",
          itemsCount: 0,
        }
      );

      expect(mockLogger.info).toHaveBeenCalledWith(
        "Order created successfully",
        {
          newStatus: "PENDING",
          totalItems: 0,
          totalValue: 0,
          leadId: expect.stringMatching(/^lead-\d+-[a-z0-9]+$/),
        }
      );
    });
  });

  describe("Performance Tracking", () => {
    test("should create performance trackers for batch and individual records", async () => {
      await handler(mockEvent);

      expect(PerformanceTracker).toHaveBeenCalledWith(
        mockLogger,
        "create-order-batch"
      );
      expect(mockTracker.finish).toHaveBeenCalledWith({
        totalRecords: 1,
        status: "completed",
      });

      expect(PerformanceTracker).toHaveBeenCalledWith(
        expect.objectContaining({ withContext: expect.any(Function) }),
        "create-order-record"
      );

      expect(PerformanceTracker).toHaveBeenCalledWith(
        expect.objectContaining({ withContext: expect.any(Function) }),
        "order-creation-logic"
      );
    });

    test("should track performance for failed records", async () => {
      mockEvent.Records[0].body = "invalid-json";

      await expect(handler(mockEvent)).rejects.toThrow();

      expect(mockTracker.finishWithError).toHaveBeenCalledWith(
        expect.any(Error)
      );
    });
  });

  describe("Logging Context", () => {
    test("should create proper logging context for each record", async () => {
      await handler(mockEvent);

      expect(mockLogger.withContext).toHaveBeenCalledWith({
        recordIndex: 0,
        messageId: "test-message-id-1",
      });

      expect(mockLogger.withContext).toHaveBeenCalledWith({
        orderId: "order-123456789-abc123",
      });
    });

    test("should log record processing details", async () => {
      await handler(mockEvent);

      expect(mockLogger.info).toHaveBeenCalledWith("Processing SQS record", {
        receiptHandle: expect.stringContaining("test-receipt-handle-1"),
        body: expect.stringContaining("Notification"),
      });

      expect(mockLogger.info).toHaveBeenCalledWith(
        "Starting order creation logic"
      );
    });
  });

  describe("Stock Management", () => {
    describe("Products with Stock Control", () => {
      test("should successfully update stock for products with quantityInStock", async () => {
        mockSend.mockImplementation((command: any) => {
          if (command.constructor.name === "QueryCommand") {
            return Promise.resolve({ Items: [] });
          }

          if (command.constructor.name === "GetCommand") {
            const productId = command.input?.Key?.id;
            if (productId === "item1") {
              return Promise.resolve({
                Item: {
                  id: "item1",
                  name: "Produto com Estoque",
                  price: 29.99,
                  category: "electronics",
                  isActive: true,
                  quantityInStock: 100,
                },
              });
            }
          }

          if (command.constructor.name === "UpdateCommand") {
            return Promise.resolve({
              Attributes: {
                id: "item1",
                quantityInStock: 95,
                updatedAt: new Date().toISOString(),
              },
            });
          }

          return Promise.resolve({});
        });

        mockEvent.Records[0].body = JSON.stringify({
          Type: "Notification",
          MessageId: "sns-message-id-1",
          TopicArn: "arn:aws:sns:us-east-1:123456789012:initialize-order-topic",
          Subject: "New Order Request",
          Message: JSON.stringify({
            ...messageData,
            items: [{ id: "item1", quantity: 5 }],
          }),
          Timestamp: "2025-08-03T10:30:00.000Z",
        });

        await handler(mockEvent);

        const updateCalls = mockSend.mock.calls.filter(
          (call: any) => call[0].constructor.name === "UpdateCommand"
        );
        expect(updateCalls).toHaveLength(1);

        const updateCommand = updateCalls[0][0];
        expect(updateCommand.input.Key.id).toBe("item1");
        expect(updateCommand.input.UpdateExpression).toContain(
          "SET quantityInStock = quantityInStock - :quantity"
        );
        expect(updateCommand.input.ExpressionAttributeValues[":quantity"]).toBe(
          5
        );
        expect(updateCommand.input.ConditionExpression).toBe(
          "quantityInStock >= :quantity AND isActive = :isActive"
        );
      });

      test("should handle insufficient stock error", async () => {
        mockSend.mockImplementation((command: any) => {
          if (command.constructor.name === "QueryCommand") {
            return Promise.resolve({ Items: [] });
          }

          if (command.constructor.name === "GetCommand") {
            return Promise.resolve({
              Item: {
                id: "item1",
                name: "Produto com Estoque Baixo",
                price: 29.99,
                category: "electronics",
                isActive: true,
                quantityInStock: 2,
              },
            });
          }

          return Promise.resolve({});
        });

        mockEvent.Records[0].body = JSON.stringify({
          Type: "Notification",
          MessageId: "sns-message-id-1",
          TopicArn: "arn:aws:sns:us-east-1:123456789012:initialize-order-topic",
          Subject: "New Order Request",
          Message: JSON.stringify({
            ...messageData,
            items: [{ id: "item1", quantity: 10 }],
          }),
          Timestamp: "2025-08-03T10:30:00.000Z",
        });

        await expect(handler(mockEvent)).rejects.toThrow(
          "Insufficient stock for product Produto com Estoque Baixo. Requested: 10, Available: 2"
        );

        const updateCalls = mockSend.mock.calls.filter(
          (call: any) => call[0].constructor.name === "UpdateCommand"
        );
        expect(updateCalls).toHaveLength(0);
      });

      test("should handle stock update failure", async () => {
        mockSend.mockImplementation((command: any) => {
          if (command.constructor.name === "QueryCommand") {
            return Promise.resolve({ Items: [] });
          }

          if (command.constructor.name === "GetCommand") {
            return Promise.resolve({
              Item: {
                id: "item1",
                name: "Produto com Estoque",
                price: 29.99,
                category: "electronics",
                isActive: true,
                quantityInStock: 100,
              },
            });
          }

          if (command.constructor.name === "UpdateCommand") {
            const error = new Error("The conditional request failed");
            error.name = "ConditionalCheckFailedException";
            throw error;
          }

          return Promise.resolve({});
        });

        mockEvent.Records[0].body = JSON.stringify({
          Type: "Notification",
          MessageId: "sns-message-id-1",
          TopicArn: "arn:aws:sns:us-east-1:123456789012:initialize-order-topic",
          Subject: "New Order Request",
          Message: JSON.stringify({
            ...messageData,
            items: [{ id: "item1", quantity: 5 }],
          }),
          Timestamp: "2025-08-03T10:30:00.000Z",
        });

        await expect(handler(mockEvent)).rejects.toThrow(
          "Stock update failed for product Produto com Estoque. Insufficient stock or product is inactive."
        );
      });
    });

    describe("Products without Stock Control", () => {
      test("should process products without quantityInStock successfully", async () => {
        mockSend.mockImplementation((command: any) => {
          if (command.constructor.name === "QueryCommand") {
            return Promise.resolve({ Items: [] });
          }

          if (command.constructor.name === "GetCommand") {
            const productId = command.input?.Key?.id;
            if (productId === "item1") {
              return Promise.resolve({
                Item: {
                  id: "item1",
                  name: "Produto sem Estoque",
                  price: 29.99,
                  category: "digital",
                  isActive: true,
                },
              });
            }
          }

          return Promise.resolve({});
        });

        mockEvent.Records[0].body = JSON.stringify({
          Type: "Notification",
          MessageId: "sns-message-id-1",
          TopicArn: "arn:aws:sns:us-east-1:123456789012:initialize-order-topic",
          Subject: "New Order Request",
          Message: JSON.stringify({
            ...messageData,
            items: [{ id: "item1", quantity: 100 }],
          }),
          Timestamp: "2025-08-03T10:30:00.000Z",
        });

        await handler(mockEvent);

        const updateCalls = mockSend.mock.calls.filter(
          (call: any) => call[0].constructor.name === "UpdateCommand"
        );
        expect(updateCalls).toHaveLength(0);

        const orderPutCalls = mockSend.mock.calls.filter(
          (call: any) =>
            call[0].constructor.name === "PutCommand" &&
            call[0].input?.Item?.id &&
            call[0].input?.Item?.items
        );
        expect(orderPutCalls).toHaveLength(1);

        const orderData = orderPutCalls[0][0].input.Item;
        expect(orderData.items[0].productName).toBe("Produto sem Estoque");
        expect(orderData.items[0].totalPrice).toBe(2999);
        expect(orderData.totalValue).toBe(2999);
      });

      test("should handle mixed products (with and without stock control)", async () => {
        mockSend.mockImplementation((command: any) => {
          if (command.constructor.name === "QueryCommand") {
            return Promise.resolve({ Items: [] });
          }

          if (command.constructor.name === "GetCommand") {
            const productId = command.input?.Key?.id;
            if (productId === "item1") {
              return Promise.resolve({
                Item: {
                  id: "item1",
                  name: "Produto com Estoque",
                  price: 29.99,
                  category: "electronics",
                  isActive: true,
                  quantityInStock: 100,
                },
              });
            } else if (productId === "item2") {
              return Promise.resolve({
                Item: {
                  id: "item2",
                  name: "Produto sem Estoque",
                  price: 15.5,
                  category: "digital",
                  isActive: true,
                },
              });
            }
          }

          if (command.constructor.name === "UpdateCommand") {
            const productId = command.input?.Key?.id;
            if (productId === "item1") {
              return Promise.resolve({
                Attributes: {
                  id: "item1",
                  quantityInStock: 95,
                  updatedAt: new Date().toISOString(),
                },
              });
            }
          }

          return Promise.resolve({});
        });

        mockEvent.Records[0].body = JSON.stringify({
          Type: "Notification",
          MessageId: "sns-message-id-1",
          TopicArn: "arn:aws:sns:us-east-1:123456789012:initialize-order-topic",
          Subject: "New Order Request",
          Message: JSON.stringify({
            ...messageData,
            items: [
              { id: "item1", quantity: 5 },
              { id: "item2", quantity: 10 },
            ],
          }),
          Timestamp: "2025-08-03T10:30:00.000Z",
        });

        await handler(mockEvent);

        const updateCalls = mockSend.mock.calls.filter(
          (call: any) => call[0].constructor.name === "UpdateCommand"
        );
        expect(updateCalls).toHaveLength(1);

        const updateCommand = updateCalls[0][0];
        expect(updateCommand.input.Key.id).toBe("item1");

        const orderPutCalls = mockSend.mock.calls.filter(
          (call: any) =>
            call[0].constructor.name === "PutCommand" &&
            call[0].input?.Item?.id &&
            call[0].input?.Item?.items
        );
        expect(orderPutCalls).toHaveLength(1);

        const orderData = orderPutCalls[0][0].input.Item;
        expect(orderData.items).toHaveLength(2);
        expect(orderData.totalValue).toBe(304.95);
      });
    });

    describe("Zero Quantity Handling", () => {
      test("should skip stock update for items with zero quantity", async () => {
        mockSend.mockImplementation((command: any) => {
          if (command.constructor.name === "QueryCommand") {
            return Promise.resolve({ Items: [] });
          }

          if (command.constructor.name === "GetCommand") {
            return Promise.resolve({
              Item: {
                id: "item1",
                name: "Produto com Estoque",
                price: 29.99,
                category: "electronics",
                isActive: true,
                quantityInStock: 100,
              },
            });
          }

          return Promise.resolve({});
        });

        mockEvent.Records[0].body = JSON.stringify({
          Type: "Notification",
          MessageId: "sns-message-id-1",
          TopicArn: "arn:aws:sns:us-east-1:123456789012:initialize-order-topic",
          Subject: "New Order Request",
          Message: JSON.stringify({
            ...messageData,
            items: [{ id: "item1", quantity: 0 }],
          }),
          Timestamp: "2025-08-03T10:30:00.000Z",
        });

        await handler(mockEvent);

        const updateCalls = mockSend.mock.calls.filter(
          (call: any) => call[0].constructor.name === "UpdateCommand"
        );
        expect(updateCalls).toHaveLength(0);
      });
    });
  });
});
