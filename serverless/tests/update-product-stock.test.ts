import { SQSEvent, SQSRecord } from "aws-lambda";
import { handler } from "../lambdas/product/update-product-stock";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import {
  createLogger,
  generateCorrelationId,
  PerformanceTracker,
} from "../shared/logger";

jest.mock("@aws-sdk/client-dynamodb");
jest.mock("@aws-sdk/lib-dynamodb", () => {
  const mockSend = jest.fn();
  return {
    DynamoDBDocumentClient: {
      from: jest.fn(() => ({
        send: mockSend,
      })),
    },
    GetCommand: jest
      .fn()
      .mockImplementation((input) => ({
        input,
        constructor: { name: "GetCommand" },
      })),
    UpdateCommand: jest
      .fn()
      .mockImplementation((input) => ({
        input,
        constructor: { name: "UpdateCommand" },
      })),
  };
});

jest.mock("../shared/logger", () => ({
  createLogger: jest.fn(),
  generateCorrelationId: jest.fn(),
  PerformanceTracker: jest.fn(),
}));

describe("update-product-stock handler", () => {
  let mockEvent: SQSEvent;
  let mockLogger: any;
  let mockTracker: any;
  let mockSend: jest.Mock;

  beforeEach(() => {
    jest.clearAllMocks();

    const mockDocClientInstance = (DynamoDBDocumentClient.from as jest.Mock)();
    mockSend = mockDocClientInstance.send as jest.Mock;
    mockSend.mockClear();

    process.env.PRODUCT_COLLECTION_TABLE = "test-product-collection";

    mockLogger = {
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
      withContext: jest.fn().mockReturnThis(),
    };
    (createLogger as jest.Mock).mockReturnValue(mockLogger);
    (generateCorrelationId as jest.Mock).mockReturnValue(
      "stock-test-correlation-id"
    );

    mockTracker = {
      finish: jest.fn(),
      finishWithError: jest.fn(),
    };
    (PerformanceTracker as jest.Mock).mockReturnValue(mockTracker);

    mockSend.mockImplementation((command: any) => {
      if (command.constructor.name === "GetCommand") {
        const productId = command.input?.Key?.productId;
        if (productId === "product-with-stock") {
          return Promise.resolve({
            Item: {
              productId: "product-with-stock",
              name: "Produto com Estoque",
              price: 50.0,
              category: "electronics",
              isActive: true,
              quantityInStock: 100,
            },
          });
        } else if (productId === "product-without-stock") {
          return Promise.resolve({
            Item: {
              productId: "product-without-stock",
              name: "Produto sem Estoque",
              price: 30.0,
              category: "digital",
              isActive: true,
            },
          });
        } else if (productId === "inactive-product") {
          return Promise.resolve({
            Item: {
              productId: "inactive-product",
              name: "Produto Inativo",
              price: 25.0,
              category: "tools",
              isActive: false,
              quantityInStock: 50,
            },
          });
        }
        return Promise.resolve({ Item: null });
      }

      if (command.constructor.name === "UpdateCommand") {
        const productId = command.input?.Key?.productId;
        const quantity =
          command.input?.ExpressionAttributeValues?.[":quantity"];
        const updateExpression = command.input?.UpdateExpression;

        let newStock = 0;
        if (productId === "product-with-stock") {
          if (updateExpression?.includes("- :quantity")) {
            newStock = 100 - quantity;
          } else if (updateExpression?.includes("+ :quantity")) {
            newStock = 100 + quantity;
          }
        }

        return Promise.resolve({
          Attributes: {
            productId: productId,
            quantityInStock: newStock,
            updatedAt: new Date().toISOString(),
          },
        });
      }

      return Promise.resolve({});
    });

    mockEvent = {
      Records: [
        {
          messageId: "test-message-id-1",
          receiptHandle: "test-receipt-handle-1",
          body: JSON.stringify({
            productId: "product-with-stock",
            quantity: 5,
            operation: "DECREASE",
            orderId: "order-123",
            reason: "Order processing",
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
            "arn:aws:sqs:us-east-1:123456789012:product-stock-queue",
          awsRegion: "us-east-1",
        } as SQSRecord,
      ],
    };
  });

  describe("Successful Stock Operations", () => {
    test("should successfully reduce stock (DECREASE operation)", async () => {
      await handler(mockEvent);

      expect(createLogger).toHaveBeenCalledWith({
        processId: "stock-test-correlation-id",
        functionName: "update-product-stock",
      });

      expect(mockLogger.info).toHaveBeenCalledWith(
        "Processing stock update batch started",
        {
          recordsCount: 1,
        }
      );

      expect(mockLogger.info).toHaveBeenCalledWith(
        "Stock update completed successfully",
        {
          productId: "product-with-stock",
          operation: "DECREASE",
          quantity: 5,
          orderId: "order-123",
        }
      );

      expect(mockSend).toHaveBeenCalledTimes(2);
    });

    test("should successfully add stock (INCREASE operation)", async () => {
      mockEvent.Records[0].body = JSON.stringify({
        productId: "product-with-stock",
        quantity: 10,
        operation: "INCREASE",
        orderId: "order-123",
        reason: "Stock rollback",
      });

      await handler(mockEvent);

      expect(mockLogger.info).toHaveBeenCalledWith(
        "Stock update message parsed successfully",
        {
          productId: "product-with-stock",
          operation: "INCREASE",
          quantity: 10,
          orderId: "order-123",
          reason: "Stock rollback",
        }
      );

      expect(mockSend).toHaveBeenCalledTimes(2);

      const updateCall = mockSend.mock.calls.find(
        (call: any) => call[0].constructor.name === "UpdateCommand"
      );
      expect(updateCall[0].input.UpdateExpression).toContain("+ :quantity");
    });

    test("should process multiple stock update messages", async () => {
      const secondRecord = {
        ...mockEvent.Records[0],
        messageId: "test-message-id-2",
        receiptHandle: "test-receipt-handle-2",
        body: JSON.stringify({
          productId: "product-with-stock",
          quantity: 3,
          operation: "INCREASE",
          orderId: "order-456",
          reason: "Inventory adjustment",
        }),
      } as SQSRecord;

      mockEvent.Records.push(secondRecord);

      await handler(mockEvent);

      expect(mockLogger.info).toHaveBeenCalledWith(
        "Processing stock update batch started",
        {
          recordsCount: 2,
        }
      );

      expect(mockLogger.info).toHaveBeenCalledWith(
        "Stock update batch processing completed",
        {
          totalRecords: 2,
        }
      );

      expect(mockSend).toHaveBeenCalledTimes(4);
    });
  });

  describe("Error Handling", () => {
    test("should handle product not found error", async () => {
      mockEvent.Records[0].body = JSON.stringify({
        productId: "non-existent-product",
        quantity: 5,
        operation: "BAIXA",
        orderId: "order-123",
      });

      await expect(handler(mockEvent)).rejects.toThrow(
        "Product not found: non-existent-product"
      );

      expect(mockLogger.error).toHaveBeenCalledWith(
        "Product not found for stock update",
        expect.any(Error),
        { productId: "non-existent-product" }
      );
    });

    test("should handle inactive product error", async () => {
      mockEvent.Records[0].body = JSON.stringify({
        productId: "inactive-product",
        quantity: 5,
        operation: "BAIXA",
        orderId: "order-123",
      });

      await expect(handler(mockEvent)).rejects.toThrow(
        "Product is inactive: inactive-product"
      );

      expect(mockLogger.error).toHaveBeenCalledWith(
        "Cannot update stock for inactive product",
        expect.any(Error),
        {
          productId: "inactive-product",
          productName: "Produto Inativo",
        }
      );
    });

    test("should handle product without stock control error", async () => {
      mockEvent.Records[0].body = JSON.stringify({
        productId: "product-without-stock",
        quantity: 5,
        operation: "BAIXA",
        orderId: "order-123",
      });

      await expect(handler(mockEvent)).rejects.toThrow(
        "Product Produto sem Estoque does not have stock control (quantityInStock property missing)"
      );

      expect(mockLogger.error).toHaveBeenCalledWith(
        "Product does not have stock control",
        expect.any(Error),
        {
          productId: "product-without-stock",
          productName: "Produto sem Estoque",
        }
      );
    });

    test("should handle invalid operation type error", async () => {
      mockEvent.Records[0].body = JSON.stringify({
        productId: "product-with-stock",
        quantity: 5,
        operation: "INVALID_OPERATION",
        orderId: "order-123",
      });

      await expect(handler(mockEvent)).rejects.toThrow(
        "Invalid operation type: INVALID_OPERATION. Must be DECREASE or INCREASE"
      );
    });

    test("should handle insufficient stock error on DECREASE operation", async () => {
      mockSend.mockImplementation((command: any) => {
        if (command.constructor.name === "GetCommand") {
          return Promise.resolve({
            Item: {
              productId: "product-with-stock",
              name: "Produto com Estoque",
              price: 50.0,
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

      await expect(handler(mockEvent)).rejects.toThrow(
        "Insufficient stock for product product-with-stock. Cannot reduce stock by 5."
      );
    });

    test("should handle invalid JSON in message body", async () => {
      mockEvent.Records[0].body = "invalid-json";

      await expect(handler(mockEvent)).rejects.toThrow();

      expect(mockLogger.error).toHaveBeenCalledWith(
        "Error processing stock update record",
        expect.any(Error),
        { sqsMessagePreview: "invalid-json" }
      );
    });
  });

  describe("Stock Operations Validation", () => {
    test("should validate DECREASE operation with correct conditions", async () => {
      mockEvent.Records[0].body = JSON.stringify({
        productId: "product-with-stock",
        quantity: 5,
        operation: "DECREASE",
        orderId: "order-123",
      });

      await handler(mockEvent);

      const updateCall = mockSend.mock.calls.find(
        (call: any) => call[0].constructor.name === "UpdateCommand"
      );

      expect(updateCall[0].input.UpdateExpression).toBe(
        "SET quantityInStock = quantityInStock - :quantity, updatedAt = :updatedAt"
      );
      expect(updateCall[0].input.ConditionExpression).toBe(
        "quantityInStock >= :quantity AND isActive = :isActive"
      );
      expect(updateCall[0].input.ExpressionAttributeValues[":quantity"]).toBe(
        5
      );
      expect(updateCall[0].input.ExpressionAttributeValues[":isActive"]).toBe(
        true
      );
    });

    test("should validate INCREASE operation with correct conditions", async () => {
      mockEvent.Records[0].body = JSON.stringify({
        productId: "product-with-stock",
        quantity: 10,
        operation: "INCREASE",
        orderId: "order-123",
      });

      await handler(mockEvent);

      const updateCall = mockSend.mock.calls.find(
        (call: any) => call[0].constructor.name === "UpdateCommand"
      );

      expect(updateCall[0].input.UpdateExpression).toBe(
        "SET quantityInStock = quantityInStock + :quantity, updatedAt = :updatedAt"
      );
      expect(updateCall[0].input.ConditionExpression).toBe(
        "isActive = :isActive"
      );
      expect(updateCall[0].input.ExpressionAttributeValues[":quantity"]).toBe(
        10
      );
      expect(updateCall[0].input.ExpressionAttributeValues[":isActive"]).toBe(
        true
      );
    });
  });

  describe("Performance Tracking", () => {
    test("should create performance trackers for batch and individual records", async () => {
      await handler(mockEvent);

      expect(PerformanceTracker).toHaveBeenCalledWith(
        mockLogger,
        "stock-update-batch"
      );
      expect(mockTracker.finish).toHaveBeenCalledWith({
        totalRecords: 1,
        status: "completed",
      });

      expect(PerformanceTracker).toHaveBeenCalledWith(
        expect.objectContaining({ withContext: expect.any(Function) }),
        "stock-update-record"
      );

      expect(PerformanceTracker).toHaveBeenCalledWith(
        expect.objectContaining({ withContext: expect.any(Function) }),
        "stock-update-logic"
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
        productId: "product-with-stock",
        operation: "DECREASE",
        quantity: 5,
      });
    });

    test("should log detailed stock update information", async () => {
      await handler(mockEvent);

      expect(mockLogger.info).toHaveBeenCalledWith(
        "Stock updated successfully",
        {
          productId: "product-with-stock",
          productName: "Produto com Estoque",
          operation: "DECREASE",
          previousStock: 100,
          newStock: 95,
          quantityChanged: 5,
          orderId: "order-123",
          reason: "Order processing",
        }
      );
    });
  });
});
