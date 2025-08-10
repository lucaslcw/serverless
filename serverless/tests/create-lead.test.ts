import { SQSEvent, SQSRecord } from "aws-lambda";
import { MessageData } from "../lambdas/order/initialize-order";

const mockSend = jest.fn();

jest.mock("@aws-sdk/client-dynamodb", () => ({
  DynamoDBClient: jest.fn().mockImplementation(() => ({})),
}));

jest.mock("@aws-sdk/lib-dynamodb", () => ({
  DynamoDBDocumentClient: {
    from: jest.fn().mockImplementation(() => ({
      send: mockSend,
    })),
  },
  QueryCommand: jest.fn().mockImplementation((params) => params),
  PutCommand: jest.fn().mockImplementation((params) => params),
}));

import { handler } from "../lambdas/lead/create-lead";

process.env.LEAD_COLLECTION_TABLE = "lead-collection-test";

describe("Create Lead Lambda - DynamoDB Integration", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    
    process.env.AWS_REGION = "us-east-1";
    process.env.LEAD_COLLECTION_TABLE = "leads-table";
    
    mockSend.mockResolvedValue({
      Items: [],
    });
  });

  const createSNSMessageEvent = (messageData: MessageData): SQSEvent => ({
    Records: [
      {
        messageId: "test-message-id",
        receiptHandle: "test-receipt-handle",
        body: JSON.stringify({
          Type: "Notification",
          Message: JSON.stringify(messageData),
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

  const createValidMessageData = (): MessageData => ({
    orderId: "test-order-123",
    customerData: {
      cpf: "12345678901",
      email: "test@example.com",
      name: "Test User",
    },
    addressData: {
      street: "Test Street",
      number: "123",
      neighborhood: "Test Neighborhood",
      city: "Test City",
      state: "TS",
      zipCode: "12345-678",
      country: "Brasil",
    },
    paymentData: {
      cardNumber: "4111111111111111",
      cardHolderName: "Test User",
      expiryMonth: "12",
      expiryYear: "2025",
      cvv: "123",
    },
    items: [
      {
        id: "prod-1",
        quantity: 1,
      },
    ],
  });

  describe("DynamoDB Lead Creation", () => {
    it("should create new lead when no existing lead found", async () => {
      const event = createSNSMessageEvent(createValidMessageData());

      await expect(handler(event)).resolves.toBeUndefined();

      expect(mockSend).toHaveBeenCalledTimes(2);
    });

    it("should skip insertion when lead with same email and CPF exists", async () => {
      mockSend.mockResolvedValueOnce({
        Items: [
          {
            id: "existing-lead-1",
            cpf: "12345678901",
            email: "test@example.com",
            name: "Existing User",
          },
        ],
      });

      const messageData = createValidMessageData();
      const event = createSNSMessageEvent(messageData);

      await expect(handler(event)).resolves.toBeUndefined();

      expect(mockSend).toHaveBeenCalledTimes(1);
    });

    it("should create new lead when email exists with different CPF", async () => {
      mockSend.mockResolvedValueOnce({
        Items: [
          {
            id: "existing-lead-1",
            cpf: "99999999999",
            email: "test@example.com",
            name: "Different User",
          },
        ],
      });

      const messageData = createValidMessageData();
      const event = createSNSMessageEvent(messageData);

      await expect(handler(event)).resolves.toBeUndefined();

      expect(mockSend).toHaveBeenCalledTimes(2);
    });

    it("should handle DynamoDB query errors gracefully", async () => {
      mockSend.mockRejectedValueOnce(new Error("DynamoDB query failed"));

      const messageData = createValidMessageData();
      const event = createSNSMessageEvent(messageData);

      await expect(handler(event)).resolves.toBeUndefined();

      expect(mockSend).toHaveBeenCalledTimes(1);
    });
  });

  describe("Error Handling", () => {
    it("should handle malformed SNS message gracefully", async () => {
      const event: SQSEvent = {
        Records: [
          {
            messageId: "test-message-malformed",
            receiptHandle: "test-receipt-malform",
            body: "invalid-json",
            attributes: {},
            messageAttributes: {},
            md5OfBody: "test-md5",
            eventSource: "aws:sqs",
            eventSourceARN: "arn:aws:sqs:us-east-1:123456789012:test-queue",
            awsRegion: "us-east-1",
          } as SQSRecord,
        ],
      };

      await expect(handler(event)).resolves.toBeUndefined();
      
      expect(mockSend).not.toHaveBeenCalled();
    });
  });
});
