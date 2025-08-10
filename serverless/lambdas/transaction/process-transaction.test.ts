import { SQSEvent, SQSRecord } from "aws-lambda";
import { handler, TransactionMessage } from "./process-transaction";

jest.mock("@aws-sdk/client-dynamodb");
jest.mock("@aws-sdk/lib-dynamodb", () => {
  const mockSend = jest.fn();
  return {
    DynamoDBDocumentClient: {
      from: jest.fn(() => ({
        send: mockSend,
      })),
    },
    GetCommand: jest.fn().mockImplementation((input) => ({
      input,
      constructor: { name: "GetCommand" },
    })),
    PutCommand: jest.fn().mockImplementation((input) => ({
      input,
      constructor: { name: "PutCommand" },
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

jest.mock("../../shared/logger", () => ({
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
} from "../../shared/logger";

import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { SQSClient } from "@aws-sdk/client-sqs";
import { OrderStatus } from "../../shared/schemas/order";
import { PaymentStatus } from "../../shared/schemas/transaction";

describe("process-transaction handler", () => {
  let mockEvent: SQSEvent;
  let mockLogger: any;
  let mockTracker: any;
  let mockSend: jest.Mock;
  let mockSqsSend: jest.Mock;

  const mockTransactionMessage: TransactionMessage = {
    orderId: "order-123456789-abc123",
    orderTotalValue: 75.49,
    paymentData: {
      cardNumber: "4111111111111111",
      cardHolderName: "João Silva",
      expiryMonth: "12",
      expiryYear: "2025",
      cvv: "123",
    },
    addressData: {
      street: "Av. Brasil",
      number: "1000",
      complement: "Apto 101",
      neighborhood: "Centro",
      city: "São Paulo",
      state: "SP",
      zipCode: "12345-678",
      country: "Brasil",
    },
    customerData: {
      name: "João Silva",
      email: "joao@example.com",
      cpf: "12345678901",
    },
  };

  const mockOrderData = {
    id: "order-123456789-abc123",
    leadId: "lead-123456789-def456",
    status: OrderStatus.PENDING,
    totalValue: 75.49,
    totalItems: 3,
    createdAt: "2025-08-10T10:00:00.000Z",
    updatedAt: "2025-08-10T10:00:00.000Z",
    name: "João Silva",
    email: "joao@example.com",
    cpf: "12345678901",
    items: [
      {
        id: "item1",
        quantity: 2,
        unitPrice: 29.99,
        totalPrice: 59.98,
        productName: "Produto A",
        hasStockControl: true,
      },
      {
        id: "item2",
        quantity: 1,
        unitPrice: 15.51,
        totalPrice: 15.51,
        productName: "Produto B",
        hasStockControl: true,
      },
    ],
    addressData: mockTransactionMessage.addressData,
  };

  beforeEach(() => {
    jest.clearAllMocks();

    process.env.ORDER_COLLECTION_TABLE = "test-order-collection";
    process.env.TRANSACTION_COLLECTION_TABLE = "test-transaction-collection";
    process.env.UPDATE_ORDER_QUEUE_URL = "test-update-order-queue";
    process.env.AWS_REGION = "us-east-1";

    const mockDocClientInstance = (DynamoDBDocumentClient.from as jest.Mock)();
    mockSend = mockDocClientInstance.send as jest.Mock;
    mockSend.mockClear();

    const mockSqsClientInstance = new (SQSClient as jest.Mock)();
    mockSqsSend = mockSqsClientInstance.send as jest.Mock;
    mockSqsSend.mockClear();

    mockLogger = {
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
      withContext: jest.fn().mockReturnThis(),
    };
    (createLogger as jest.Mock).mockReturnValue(mockLogger);
    (generateCorrelationId as jest.Mock).mockReturnValue(
      "transaction-test-correlation-id"
    );

    mockTracker = {
      finish: jest.fn(),
      finishWithError: jest.fn(),
    };
    (PerformanceTracker as jest.Mock).mockReturnValue(mockTracker);

    (maskSensitiveData.cpf as jest.Mock).mockReturnValue("123***901");
    (maskSensitiveData.email as jest.Mock).mockReturnValue(
      "joao***@example.com"
    );
    (maskSensitiveData.name as jest.Mock).mockReturnValue("Jo***");

    mockEvent = {
      Records: [
        {
          messageId: "test-message-id-1",
          receiptHandle: "test-receipt-handle-1",
          body: JSON.stringify(mockTransactionMessage),
          attributes: {
            ApproximateReceiveCount: "1",
            SentTimestamp: "1628623200000",
            SenderId: "test-sender-id",
            ApproximateFirstReceiveTimestamp: "1628623200000",
          },
          messageAttributes: {},
          md5OfBody: "test-md5",
          eventSource: "aws:sqs",
          eventSourceARN: "arn:aws:sqs:us-east-1:123456789012:test-queue",
          awsRegion: "us-east-1",
        },
      ] as SQSRecord[],
    };
  });

  describe("Successful Payment Processing", () => {
    beforeEach(() => {
      mockSend.mockImplementation((command: any) => {
        if (command.constructor.name === "GetCommand") {
          return Promise.resolve({ Item: mockOrderData });
        }
        if (command.constructor.name === "PutCommand") {
          return Promise.resolve({});
        }
        return Promise.resolve({});
      });

      mockSqsSend.mockResolvedValue({
        MessageId: "test-message-id",
        MD5OfBody: "test-md5",
      });
    });

    test("should process transaction with approved payment successfully", async () => {
      await handler(mockEvent);

      expect(createLogger).toHaveBeenCalledWith({
        processId: "transaction-test-correlation-id",
        functionName: "process-transaction",
      });

      expect(mockLogger.info).toHaveBeenCalledWith(
        "Processing transaction batch started",
        { recordsCount: 1 }
      );

      const orderLookupCall = mockSend.mock.calls.find(
        (call) => call[0].constructor.name === "GetCommand"
      );
      expect(orderLookupCall).toBeTruthy();
      expect(orderLookupCall[0].input.Key.orderId).toBe("order-123456789-abc123");

      const transactionPutCall = mockSend.mock.calls.find(
        (call) => call[0].constructor.name === "PutCommand"
      );
      expect(transactionPutCall).toBeTruthy();
      expect(transactionPutCall[0].input.ConditionExpression).toBe("attribute_not_exists(id)");

      const orderUpdateCall = mockSqsSend.mock.calls[0];
      expect(orderUpdateCall).toBeTruthy();
      expect(orderUpdateCall[0].input.MessageBody).toContain('"status":"PROCESSED"');

      expect(mockLogger.info).toHaveBeenCalledWith(
        "Transaction batch processing completed",
        { totalRecords: 1 }
      );
    });

    test("should mask sensitive data in transaction record", async () => {
      await handler(mockEvent);

      const transactionPutCall = mockSend.mock.calls.find(
        (call) => call[0].constructor.name === "PutCommand"
      );

      expect(transactionPutCall).toBeTruthy();
      const transactionItem = transactionPutCall[0].input.Item;

      expect(transactionItem.paymentData.cardNumber).toBe("****-****-****-1111");
      expect(transactionItem.paymentData.cvv).toBe("***");
      expect(transactionItem.customerData.cpf).toBe("123***901");
    });
  });

  describe("Payment Declined", () => {
    beforeEach(() => {
      const declinedMessage = {
        ...mockTransactionMessage,
        paymentData: {
          ...mockTransactionMessage.paymentData,
          cardNumber: "4111111111110000",
        },
      };

      mockEvent.Records[0].body = JSON.stringify(declinedMessage);

      mockSend.mockImplementation((command: any) => {
        if (command.constructor.name === "GetCommand") {
          return Promise.resolve({ Item: mockOrderData });
        }
        if (command.constructor.name === "PutCommand") {
          return Promise.resolve({});
        }
        return Promise.resolve({});
      });

      mockSqsSend.mockResolvedValue({
        MessageId: "test-message-id",
        MD5OfBody: "test-md5",
      });
    });

    test("should process declined payment correctly", async () => {
      await handler(mockEvent);

      const transactionPutCall = mockSend.mock.calls.find(
        (call) => call[0].constructor.name === "PutCommand"
      );

      expect(transactionPutCall).toBeTruthy();
      const transactionItem = transactionPutCall[0].input.Item;
      expect(transactionItem.paymentStatus).toBe(PaymentStatus.DECLINED);

      const orderUpdateCall = mockSqsSend.mock.calls[0];
      expect(orderUpdateCall).toBeTruthy();
      expect(orderUpdateCall[0].input.MessageBody).toContain('"status":"CANCELLED"');
    });
  });

  describe("Error Scenarios", () => {
    test("should handle order not found", async () => {
      mockSend.mockImplementation((command: any) => {
        if (command.constructor.name === "GetCommand") {
          return Promise.resolve({ Item: null });
        }
        return Promise.resolve({});
      });

      await handler(mockEvent);

      expect(mockLogger.error).toHaveBeenCalledWith(
        "Error processing transaction record",
        expect.any(Error),
        expect.objectContaining({
          errorType: "Error",
        })
      );
    });

    test("should handle invalid message format", async () => {
      const invalidMessage = {
        Records: [
          {
            ...mockEvent.Records[0],
            body: JSON.stringify({ orderId: "test" }),
          },
        ],
      } as SQSEvent;

      await handler(invalidMessage);

      expect(mockLogger.error).toHaveBeenCalledWith(
        "Error processing transaction record",
        expect.objectContaining({
          message: expect.stringContaining("Missing required fields"),
        }),
        expect.any(Object)
      );
    });

    test("should handle DynamoDB errors gracefully", async () => {
      mockSend.mockRejectedValue(new Error("DynamoDB connection error"));

      await handler(mockEvent);

      expect(mockLogger.error).toHaveBeenCalledWith(
        "Error processing transaction record",
        expect.objectContaining({
          message: "DynamoDB connection error",
        }),
        expect.any(Object)
      );

      expect(mockTracker.finishWithError).toHaveBeenCalledWith(
        expect.any(Error)
      );
    });

    test("should handle SQS send message errors", async () => {
      mockSend.mockImplementation((command: any) => {
        if (command.constructor.name === "GetCommand") {
          return Promise.resolve({ Item: mockOrderData });
        }
        if (command.constructor.name === "PutCommand") {
          return Promise.resolve({});
        }
        return Promise.resolve({});
      });

      mockSqsSend.mockRejectedValue(new Error("SQS send error"));

      await handler(mockEvent);

      expect(mockLogger.error).toHaveBeenCalledWith(
        "Failed to send order update message",
        expect.objectContaining({
          message: "SQS send error",
        }),
        expect.any(Object)
      );
    });

    test("should create error transaction record on processing failure", async () => {
      mockSend.mockImplementation((command: any) => {
        if (command.constructor.name === "GetCommand") {
          return Promise.resolve({ Item: mockOrderData });
        }
        if (command.constructor.name === "PutCommand") {
          if (mockSend.mock.calls.filter(call => call[0].constructor.name === "PutCommand").length === 1) {
            throw new Error("Transaction creation failed");
          }
          return Promise.resolve({});
        }
        return Promise.resolve({});
      });

      mockSqsSend.mockResolvedValue({
        MessageId: "test-message-id",
        MD5OfBody: "test-md5",
      });

      await handler(mockEvent);

      const putCalls = mockSend.mock.calls.filter(
        (call) => call[0].constructor.name === "PutCommand"
      );
      expect(putCalls.length).toBe(2);

      const errorTransactionItem = putCalls[1][0].input.Item;
      expect(errorTransactionItem.paymentStatus).toBe(PaymentStatus.ERROR);
    });
  });

  describe("Multiple Records Processing", () => {
    test("should process multiple SQS records", async () => {
      const multiRecordEvent: SQSEvent = {
        Records: [
          mockEvent.Records[0],
          {
            ...mockEvent.Records[0],
            messageId: "test-message-id-2",
            receiptHandle: "test-receipt-handle-2",
            body: JSON.stringify({
              ...mockTransactionMessage,
              orderId: "order-987654321-xyz789",
            }),
          },
        ] as SQSRecord[],
      };

      mockSend.mockImplementation((command: any) => {
        if (command.constructor.name === "GetCommand") {
          const orderId = command.input.Key.orderId;
          return Promise.resolve({
            Item: {
              ...mockOrderData,
              id: orderId,
            },
          });
        }
        if (command.constructor.name === "PutCommand") {
          return Promise.resolve({});
        }
        return Promise.resolve({});
      });

      mockSqsSend.mockResolvedValue({
        MessageId: "test-message-id",
        MD5OfBody: "test-md5",
      });

      await handler(multiRecordEvent);

      expect(mockLogger.info).toHaveBeenCalledWith(
        "Processing transaction batch started",
        { recordsCount: 2 }
      );

      expect(mockLogger.info).toHaveBeenCalledWith(
        "Transaction batch processing completed",
        { totalRecords: 2 }
      );

      const orderLookupCalls = mockSend.mock.calls.filter(
        (call) => call[0].constructor.name === "GetCommand"
      );
      expect(orderLookupCalls.length).toBe(2);
    });
  });

  describe("Payment Gateway Simulation", () => {
    beforeEach(() => {
      mockSend.mockImplementation((command: any) => {
        if (command.constructor.name === "GetCommand") {
          return Promise.resolve({ Item: mockOrderData });
        }
        if (command.constructor.name === "PutCommand") {
          return Promise.resolve({});
        }
        return Promise.resolve({});
      });

      mockSqsSend.mockResolvedValue({
        MessageId: "test-message-id",
        MD5OfBody: "test-md5",
      });
    });

    test("should include processing time in transaction record", async () => {
      await handler(mockEvent);

      const transactionPutCall = mockSend.mock.calls.find(
        (call) => call[0].constructor.name === "PutCommand"
      );

      expect(transactionPutCall).toBeTruthy();
      const transactionItem = transactionPutCall[0].input.Item;
      expect(typeof transactionItem.processingTime).toBe("number");
      expect(transactionItem.processingTime).toBeGreaterThanOrEqual(0);
    });

    test("should generate auth code for approved payments", async () => {
      await handler(mockEvent);

      const transactionPutCall = mockSend.mock.calls.find(
        (call) => call[0].constructor.name === "PutCommand"
      );

      expect(transactionPutCall).toBeTruthy();
      const transactionItem = transactionPutCall[0].input.Item;
      
      if (transactionItem.paymentStatus === PaymentStatus.APPROVED) {
        expect(transactionItem.authCode).toMatch(/^AUTH-\d+$/);
      }
    });
  });

  describe("Message Validation", () => {
    test("should validate required fields", async () => {
      const invalidMessages = [
        { orderId: "test" },
        { 
          orderId: "test",
          orderTotalValue: 100,
          paymentData: { cardNumber: "1234" },
          addressData: {},
          customerData: {}
        },
        {
          orderId: "test",
          orderTotalValue: 100,
          paymentData: {
            cardNumber: "4111111111111111",
            cvv: "123",
          },
          addressData: {},
          customerData: {}
        }
      ];

      for (const invalidMessage of invalidMessages) {
        const invalidEvent: SQSEvent = {
          Records: [
            {
              ...mockEvent.Records[0],
              body: JSON.stringify(invalidMessage),
            },
          ] as SQSRecord[],
        };

        await handler(invalidEvent);

        expect(mockLogger.error).toHaveBeenCalledWith(
          "Error processing transaction record",
          expect.objectContaining({
            message: expect.stringContaining("Missing"),
          }),
          expect.any(Object)
        );
      }
    });
  });
});
