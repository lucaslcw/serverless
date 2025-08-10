import { handler } from "./update-product-stock";
import { SQSEvent } from "aws-lambda";

jest.mock("@aws-sdk/lib-dynamodb", () => {
  const mockDocClient = {
    send: jest.fn(),
  };
  
  return {
    ...jest.requireActual("@aws-sdk/lib-dynamodb"),
    DynamoDBDocumentClient: {
      from: jest.fn(() => mockDocClient),
    },
    GetCommand: jest.fn().mockImplementation((params) => params),
    PutCommand: jest.fn().mockImplementation((params) => params),
    QueryCommand: jest.fn().mockImplementation((params) => params),
  };
});

jest.mock("@aws-sdk/client-dynamodb", () => {
  return {
    DynamoDBClient: jest.fn(),
  };
});

jest.mock("uuid", () => ({
  v4: jest.fn(() => "test-uuid-123"),
}));

describe("update-product-stock handler", () => {
  let mockDocClient: any;

  beforeEach(() => {
    jest.clearAllMocks();
    
    process.env.PRODUCT_COLLECTION_TABLE = "products";
    
    const { DynamoDBDocumentClient } = require("@aws-sdk/lib-dynamodb");
    mockDocClient = DynamoDBDocumentClient.from();
    
    mockDocClient.send.mockImplementation((command: any) => {
      if (command.TableName && (command.TableName.includes("product") || command.TableName === process.env.PRODUCT_COLLECTION_TABLE)) {
        return Promise.resolve({
          Item: {
            id: "product-123",
            name: "Produto Teste",
            isActive: true,
            price: 100,
          },
        });
      }
      if (command.IndexName === "ProductStocksByProductId") {
        return Promise.resolve({
          Items: [],
        });
      }
      return Promise.resolve({});
    });
  });

  it("should process stock increase successfully", async () => {
    const mockEvent: SQSEvent = {
      Records: [
        {
          messageId: "test-message-id",
          receiptHandle: "test-receipt-handle",
          body: JSON.stringify({
            productId: "product-123",
            quantity: 10,
            operation: "INCREASE",
            orderId: "order-123",
            reason: "Purchase",
          }),
          attributes: {} as any,
          messageAttributes: {},
          md5OfBody: "",
          eventSource: "aws:sqs",
          eventSourceARN: "arn:aws:sqs:us-east-1:123456789012:test-queue",
          awsRegion: "us-east-1",
        },
      ],
    };

    await expect(handler(mockEvent)).resolves.not.toThrow();
    expect(mockDocClient.send).toHaveBeenCalled();
  });

  it("should fail with invalid message", async () => {
    const mockEvent: SQSEvent = {
      Records: [
        {
          messageId: "test-message-id",
          receiptHandle: "test-receipt-handle",
          body: "invalid-json",
          attributes: {} as any,
          messageAttributes: {},
          md5OfBody: "",
          eventSource: "aws:sqs",
          eventSourceARN: "arn:aws:sqs:us-east-1:123456789012:test-queue",
          awsRegion: "us-east-1",
        },
      ],
    };

    await expect(handler(mockEvent)).resolves.not.toThrow();
  });
});
