import { SQSEvent, SQSRecord } from "aws-lambda";

const mockSend = jest.fn();

jest.mock("@aws-sdk/client-dynamodb", () => ({
  DynamoDBClient: jest.fn().mockImplementation(() => ({})),
}));

jest.mock("@aws-sdk/lib-dynamodb", () => ({
  DynamoDBDocumentClient: {
    from: jest.fn().mockReturnValue({
      send: mockSend,
    }),
  },
  QueryCommand: jest.fn(),
  PutCommand: jest.fn(),
}));

jest.mock("../shared/logger", () => ({
  createLogger: jest.fn(() => ({
    info: jest.fn(),
    error: jest.fn(),
    withContext: jest.fn().mockReturnThis(),
  })),
  generateCorrelationId: jest.fn(() => "test-correlation-id"),
  maskSensitiveData: {
    cpf: jest.fn((cpf) => cpf ? `***${cpf.slice(-3)}` : ""),
    email: jest.fn((email) => email ? `***@${email.split("@")[1]}` : ""),
    name: jest.fn((name) => name ? `${name.substring(0, 2)}***` : ""),
  },
  PerformanceTracker: jest.fn().mockImplementation(() => ({
    finish: jest.fn(),
    finishWithError: jest.fn(),
  })),
}));

import { handler } from "../lambdas/lead/create-lead";

process.env.LEAD_COLLECTION_TABLE = "lead-collection-test";

describe("Create Lead Lambda - DynamoDB Integration", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  const createSNSMessageEvent = (orderData: any): SQSEvent => ({
    Records: [
      {
        messageId: "test-message-id",
        receiptHandle: "test-receipt-handle",
        body: JSON.stringify({
          Type: "Notification",
          Message: JSON.stringify(orderData),
        }),
        attributes: {},
        messageAttributes: {},
        md5OfBody: "test-md5",
        eventSource: "aws:sqs",
        eventSourceARN: "arn:aws:sqs:us-east-1:123456789012:test-queue",
        awsRegion: "us-east-1",
      } as SQSRecord,
    ],
  });

  const createValidOrderData = () => ({
    orderId: "order-123",
    cpf: "12345678901",
    email: "test@example.com",
    name: "João Silva",
    items: [
      { id: "item-1", quantity: 2 },
      { id: "item-2", quantity: 1 },
    ],
    timestamp: "2023-12-01T10:00:00Z",
    status: "confirmed",
  });

  describe("DynamoDB Lead Creation", () => {
    it("should create new lead when no existing lead found", async () => {
      mockSend.mockResolvedValueOnce({
        Items: [],
      });

      mockSend.mockResolvedValueOnce({});

      const event = createSNSMessageEvent(createValidOrderData());
      
      await expect(handler(event)).resolves.toBeUndefined();
      
      expect(mockSend).toHaveBeenCalledTimes(2);
    });

    it("should skip insertion when lead with same email and CPF exists", async () => {
      const orderData = createValidOrderData();
      
      mockSend.mockResolvedValueOnce({
        Items: [
          {
            id: "existing-lead-123",
            customerCpf: orderData.cpf,
            customerEmail: orderData.email,
            createdAt: "2023-11-01T10:00:00Z",
            updatedAt: "2023-11-01T10:00:00Z",
          },
        ],
      });

      const event = createSNSMessageEvent(orderData);
      
      await expect(handler(event)).resolves.toBeUndefined();
      
      expect(mockSend).toHaveBeenCalledTimes(1);
    });

    it("should create new lead when email exists with different CPF", async () => {
      const orderData = createValidOrderData();
      
      mockSend.mockResolvedValueOnce({
        Items: [
          {
            id: "existing-lead-123",
            customerCpf: "98765432100",
            customerEmail: orderData.email,
            createdAt: "2023-11-01T10:00:00Z",
            updatedAt: "2023-11-01T10:00:00Z",
          },
        ],
      });

      mockSend.mockResolvedValueOnce({});

      const event = createSNSMessageEvent(orderData);
      
      await expect(handler(event)).resolves.toBeUndefined();
      
      expect(mockSend).toHaveBeenCalledTimes(2);
    });

    it("should create new lead when multiple leads exist with same email but different CPFs", async () => {
      const orderData = createValidOrderData();
      
      mockSend.mockResolvedValueOnce({
        Items: [
          {
            id: "existing-lead-1",
            customerCpf: "98765432100",
            customerEmail: orderData.email,
            createdAt: "2023-11-01T10:00:00Z",
            updatedAt: "2023-11-01T10:00:00Z",
          },
          {
            id: "existing-lead-2",
            customerCpf: "11111111111",
            customerEmail: orderData.email,
            createdAt: "2023-11-02T10:00:00Z",
            updatedAt: "2023-11-02T10:00:00Z",
          },
        ],
      });

      mockSend.mockResolvedValueOnce({});

      const event = createSNSMessageEvent(orderData);
      
      await expect(handler(event)).resolves.toBeUndefined();
      
      expect(mockSend).toHaveBeenCalledTimes(2);
    });

    it("should handle DynamoDB query errors gracefully", async () => {
      const orderData = createValidOrderData();
      
      mockSend.mockRejectedValueOnce(new Error("DynamoDB query failed"));

      const event = createSNSMessageEvent(orderData);
      
      await expect(handler(event)).rejects.toThrow("DynamoDB query failed");
      
      expect(mockSend).toHaveBeenCalledTimes(1);
    });

    it("should handle DynamoDB put errors gracefully", async () => {
      const orderData = createValidOrderData();
      
      mockSend.mockResolvedValueOnce({
        Items: [],
      });

      mockSend.mockRejectedValueOnce(new Error("DynamoDB put failed"));

      const event = createSNSMessageEvent(orderData);
      
      await expect(handler(event)).rejects.toThrow("DynamoDB put failed");
      
      expect(mockSend).toHaveBeenCalledTimes(2);
    });
  });

  describe("Lead Data Validation", () => {
    it("should process lead with valid data structure", async () => {
      mockSend.mockResolvedValueOnce({
        Items: [],
      });

      mockSend.mockResolvedValueOnce({});

      const orderData = {
        orderId: "order-456",
        cpf: "12345678901",
        email: "new-customer@example.com",
        name: "Maria Santos",
        items: [
          { id: "product-1", quantity: 3 },
          { id: "product-2", quantity: 2 },
        ],
        timestamp: "2023-12-01T15:30:00Z",
        status: "pending",
      };

      const event = createSNSMessageEvent(orderData);
      
      await expect(handler(event)).resolves.toBeUndefined();
      
      expect(mockSend).toHaveBeenCalledTimes(2);
    });

    it("should handle empty items array", async () => {
      mockSend.mockResolvedValueOnce({
        Items: [],
      });

      mockSend.mockResolvedValueOnce({});

      const orderData = {
        orderId: "order-empty-items",
        cpf: "12345678901",
        email: "empty-items@example.com",
        name: "Carlos Lima",
        items: [],
        timestamp: "2023-12-01T16:00:00Z",
        status: "confirmed",
      };

      const event = createSNSMessageEvent(orderData);
      
      await expect(handler(event)).resolves.toBeUndefined();
      
      expect(mockSend).toHaveBeenCalledTimes(2);
    });

    it("should handle missing items property", async () => {
      mockSend.mockResolvedValueOnce({
        Items: [],
      });

      mockSend.mockResolvedValueOnce({});

      const orderData = {
        orderId: "order-no-items",
        cpf: "12345678901",
        email: "no-items@example.com",
        name: "Ana Costa",
        timestamp: "2023-12-01T17:00:00Z",
        status: "confirmed",
      };

      const event = createSNSMessageEvent(orderData);
      
      await expect(handler(event)).resolves.toBeUndefined();
      
      expect(mockSend).toHaveBeenCalledTimes(2);
    });
  });

  describe("Batch Processing", () => {
    it("should process multiple records in batch", async () => {
      const orderData1 = {
        orderId: "order-batch-1",
        cpf: "11111111111",
        email: "batch1@example.com",
        name: "Pedro Oliveira",
        items: [{ id: "item-1", quantity: 1 }],
        timestamp: "2023-12-01T10:00:00Z",
        status: "confirmed",
      };

      const orderData2 = {
        orderId: "order-batch-2",
        cpf: "22222222222",
        email: "batch2@example.com",
        name: "Lucia Ferreira",
        items: [{ id: "item-2", quantity: 2 }],
        timestamp: "2023-12-01T10:05:00Z",
        status: "confirmed",
      };

      mockSend
        .mockResolvedValueOnce({ Items: [] })
        .mockResolvedValueOnce({})
        .mockResolvedValueOnce({ Items: [] })
        .mockResolvedValueOnce({});

      const event: SQSEvent = {
        Records: [
          {
            messageId: "test-message-1",
            receiptHandle: "test-receipt-1",
            body: JSON.stringify({
              Type: "Notification",
              Message: JSON.stringify(orderData1),
            }),
            attributes: {},
            messageAttributes: {},
            md5OfBody: "test-md5-1",
            eventSource: "aws:sqs",
            eventSourceARN: "arn:aws:sqs:us-east-1:123456789012:test-queue",
            awsRegion: "us-east-1",
          } as SQSRecord,
          {
            messageId: "test-message-2",
            receiptHandle: "test-receipt-2",
            body: JSON.stringify({
              Type: "Notification",
              Message: JSON.stringify(orderData2),
            }),
            attributes: {},
            messageAttributes: {},
            md5OfBody: "test-md5-2",
            eventSource: "aws:sqs",
            eventSourceARN: "arn:aws:sqs:us-east-1:123456789012:test-queue",
            awsRegion: "us-east-1",
          } as SQSRecord,
        ],
      };

      await expect(handler(event)).resolves.toBeUndefined();
      
      expect(mockSend).toHaveBeenCalledTimes(4);
    });

    it("should handle mixed scenarios in batch (some insert, some skip)", async () => {
      const orderData1 = {
        orderId: "order-mixed-1",
        cpf: "33333333333",
        email: "mixed1@example.com",
        name: "Roberto Silva",
        items: [{ id: "item-1", quantity: 1 }],
        timestamp: "2023-12-01T11:00:00Z",
        status: "confirmed",
      };

      const orderData2 = {
        orderId: "order-mixed-2",
        cpf: "44444444444",
        email: "mixed2@example.com",
        name: "Fernanda Souza",
        items: [{ id: "item-2", quantity: 2 }],
        timestamp: "2023-12-01T11:05:00Z",
        status: "confirmed",
      };

      mockSend
        .mockResolvedValueOnce({ Items: [] })
        .mockResolvedValueOnce({})
        .mockResolvedValueOnce({
          Items: [
            {
              id: "existing-lead-mixed",
              customerCpf: orderData2.cpf,
              customerEmail: orderData2.email,
              createdAt: "2023-11-01T10:00:00Z",
              updatedAt: "2023-11-01T10:00:00Z",
            },
          ],
        });

      const event: SQSEvent = {
        Records: [
          {
            messageId: "test-message-mixed-1",
            receiptHandle: "test-receipt-mixed-1",
            body: JSON.stringify({
              Type: "Notification",
              Message: JSON.stringify(orderData1),
            }),
            attributes: {},
            messageAttributes: {},
            md5OfBody: "test-md5-mixed-1",
            eventSource: "aws:sqs",
            eventSourceARN: "arn:aws:sqs:us-east-1:123456789012:test-queue",
            awsRegion: "us-east-1",
          } as SQSRecord,
          {
            messageId: "test-message-mixed-2",
            receiptHandle: "test-receipt-mixed-2",
            body: JSON.stringify({
              Type: "Notification",
              Message: JSON.stringify(orderData2),
            }),
            attributes: {},
            messageAttributes: {},
            md5OfBody: "test-md5-mixed-2",
            eventSource: "aws:sqs",
            eventSourceARN: "arn:aws:sqs:us-east-1:123456789012:test-queue",
            awsRegion: "us-east-1",
          } as SQSRecord,
        ],
      };

      await expect(handler(event)).resolves.toBeUndefined();
      
      expect(mockSend).toHaveBeenCalledTimes(3);
    });
  });

  describe("Error Handling", () => {
    it("should handle malformed SNS message", async () => {
      const event: SQSEvent = {
        Records: [
          {
            messageId: "test-message-malformed",
            receiptHandle: "test-receipt-malformed",
            body: "invalid-json",
            attributes: {},
            messageAttributes: {},
            md5OfBody: "test-md5-malformed",
            eventSource: "aws:sqs",
            eventSourceARN: "arn:aws:sqs:us-east-1:123456789012:test-queue",
            awsRegion: "us-east-1",
          } as SQSRecord,
        ],
      };

      await expect(handler(event)).rejects.toThrow();
      
      expect(mockSend).toHaveBeenCalledTimes(0);
    });

    it("should handle missing order data fields", async () => {
      const incompleteOrderData = {
        orderId: "order-incomplete",
        items: [{ id: "item-1", quantity: 1 }],
        timestamp: "2023-12-01T12:00:00Z",
        status: "confirmed",
      };

      mockSend.mockResolvedValueOnce({
        Items: [],
      });

      mockSend.mockResolvedValueOnce({});

      const event = createSNSMessageEvent(incompleteOrderData);
      
      await expect(handler(event)).resolves.toBeUndefined();
      
      expect(mockSend).toHaveBeenCalledTimes(2);
    });
  });
});
