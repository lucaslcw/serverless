import { APIGatewayProxyEvent } from "aws-lambda";

const mockPublish = jest.fn();
jest.mock("aws-sdk", () => ({
  SNS: jest.fn().mockImplementation(() => ({
    publish: mockPublish,
  })),
}));

jest.mock("../../shared/validators", () => ({
  validateInitializeOrderData: jest.fn(),
  sanitizeInitializeOrderData: jest.fn(),
  createErrorResponse: jest.fn(),
  createSuccessResponse: jest.fn(),
}));

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

import { handler } from "../order/initialize-order";
import {
  validateInitializeOrderData,
  sanitizeInitializeOrderData,
  createErrorResponse,
  createSuccessResponse,
} from "../../shared/validators";

import {
  createLogger,
  generateCorrelationId,
  maskSensitiveData,
  PerformanceTracker,
} from "../../shared/logger";

describe("initialize-order handler", () => {
  let mockEvent: Partial<APIGatewayProxyEvent>;
  let mockLogger: any;
  let mockTracker: any;

  beforeEach(() => {
    jest.clearAllMocks();

    mockPublish.mockReturnValue({
      promise: jest.fn().mockResolvedValue({ MessageId: "test-message-id" }),
    });

    mockLogger = {
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
      withContext: jest.fn().mockReturnThis(),
    };
    (createLogger as jest.Mock).mockReturnValue(mockLogger);
    (generateCorrelationId as jest.Mock).mockReturnValue("test-correlation-id");

    mockTracker = {
      finish: jest.fn(),
      finishWithError: jest.fn(),
    };
    (PerformanceTracker as jest.Mock).mockReturnValue(mockTracker);

    (maskSensitiveData.cpf as jest.Mock).mockReturnValue("123***");
    (maskSensitiveData.email as jest.Mock).mockReturnValue(
      "test***@example.com"
    );
    (maskSensitiveData.name as jest.Mock).mockReturnValue("Jo*** S***");

    mockEvent = {
      httpMethod: "POST",
      path: "/orders",
      body: JSON.stringify({
        customerData: {
          cpf: "12345678901",
          email: "test@example.com",
          name: "João Silva",
        },
        items: [{ id: "item1", quantity: 2 }],
      }),
      headers: {
        "User-Agent": "test-agent",
      },
      requestContext: {
        identity: {
          sourceIp: "127.0.0.1",
        },
      } as any,
    };

    process.env.INITIALIZE_ORDER_TOPIC_ARN =
      "arn:aws:sns:us-east-1:123456789012:test-topic";
  });

  afterEach(() => {
    delete process.env.INITIALIZE_ORDER_TOPIC_ARN;
  });

  describe("successful order creation", () => {
    beforeEach(() => {
      (validateInitializeOrderData as jest.Mock).mockReturnValue({
        isValid: true,
      });
      (sanitizeInitializeOrderData as jest.Mock).mockReturnValue({
        customerData: {
          cpf: "12345678901",
          email: "test@example.com",
          name: "João Silva",
        },
        items: [{ id: "item1", quantity: 2 }],
      });
      (createSuccessResponse as jest.Mock).mockReturnValue({
        statusCode: 202,
        body: JSON.stringify({ message: "Success", orderId: "test-order-id" }),
      });
    });

    test("should initialize order successfully", async () => {
      const result = await handler(mockEvent as APIGatewayProxyEvent);

      expect(validateInitializeOrderData).toHaveBeenCalledWith({
        customerData: {
          cpf: "12345678901",
          email: "test@example.com",
          name: "João Silva",
        },
        items: [{ id: "item1", quantity: 2 }],
      });

      expect(sanitizeInitializeOrderData).toHaveBeenCalled();
      expect(mockPublish).toHaveBeenCalled();
      expect(createSuccessResponse).toHaveBeenCalledWith(
        202,
        expect.objectContaining({
          message: "Order request submitted successfully",
          status: "submitted",
        })
      );

      expect(result.statusCode).toBe(202);
    });

    test("should log request details", async () => {
      await handler(mockEvent as APIGatewayProxyEvent);

      expect(createLogger).toHaveBeenCalledWith({
        correlationId: "test-correlation-id",
        functionName: "initialize-order",
        httpMethod: "POST",
        path: "/orders",
      });

      expect(mockLogger.info).toHaveBeenCalledWith("Request received", {
        sourceIp: "127.0.0.1",
        userAgent: "test-agent",
        bodyLength: expect.any(Number),
      });
    });

    test("should track performance", async () => {
      await handler(mockEvent as APIGatewayProxyEvent);

      expect(PerformanceTracker).toHaveBeenCalledWith(
        mockLogger,
        "initialize-order-request"
      );
      expect(mockTracker.finish).toHaveBeenCalledWith({
        statusCode: 202,
        orderId: expect.stringMatching(/^order-\d+-[a-z0-9]+$/),
      });
    });

    test("should publish to SNS with correct parameters", async () => {
      await handler(mockEvent as APIGatewayProxyEvent);

      expect(mockPublish).toHaveBeenCalledWith({
        TopicArn: "arn:aws:sns:us-east-1:123456789012:test-topic",
        Message: expect.stringContaining(
          '"customerData":{"cpf":"12345678901","email":"test@example.com","name":"João Silva"}'
        ),
        Subject: "New Order Request",
        MessageAttributes: {
          messageType: {
            DataType: "String",
            StringValue: "order_initialized",
          },
        },
      });
    });

    test("should mask sensitive data in logs", async () => {
      await handler(mockEvent as APIGatewayProxyEvent);

      expect(maskSensitiveData.cpf).toHaveBeenCalled();
      expect(maskSensitiveData.email).toHaveBeenCalled();
    });
  });

  describe("validation failures", () => {
    test("should handle validation errors", async () => {
      (validateInitializeOrderData as jest.Mock).mockReturnValue({
        isValid: false,
        error: "Invalid CPF format",
      });
      (createErrorResponse as jest.Mock).mockReturnValue({
        statusCode: 400,
        body: JSON.stringify({ error: "Invalid CPF format" }),
      });

      const result = await handler(mockEvent as APIGatewayProxyEvent);

      expect(mockLogger.warn).toHaveBeenCalledWith("Validation failed", {
        error: "Invalid CPF format",
        receivedFields: ["customerData", "items"],
      });

      expect(mockTracker.finishWithError).toHaveBeenCalledWith(
        expect.objectContaining({ message: "Invalid CPF format" })
      );

      expect(createErrorResponse).toHaveBeenCalledWith(
        400,
        "Invalid CPF format"
      );
      expect(result.statusCode).toBe(400);
    });
  });

  describe("error handling", () => {
    test("should handle JSON parsing errors", async () => {
      mockEvent.body = "invalid-json";
      (createErrorResponse as jest.Mock).mockReturnValue({
        statusCode: 500,
        body: JSON.stringify({ error: "Internal server error" }),
      });

      const result = await handler(mockEvent as APIGatewayProxyEvent);

      expect(mockLogger.error).toHaveBeenCalledWith(
        "Unexpected error occurred",
        expect.any(Error),
        {
          hasBody: true,
          bodyLength: 12,
        }
      );

      expect(mockTracker.finishWithError).toHaveBeenCalled();
      expect(createErrorResponse).toHaveBeenCalledWith(
        500,
        "Internal server error"
      );
      expect(result.statusCode).toBe(500);
    });

    test("should handle SNS publish errors", async () => {
      (validateInitializeOrderData as jest.Mock).mockReturnValue({
        isValid: true,
      });
      (sanitizeInitializeOrderData as jest.Mock).mockReturnValue({
        customerData: {
          cpf: "12345678901",
          email: "test@example.com",
          name: "João Silva",
        },
        items: [{ id: "item1", quantity: 2 }],
      });

      const snsError = new Error("SNS publish failed");
      mockPublish.mockReturnValue({
        promise: jest.fn().mockRejectedValue(snsError),
      });

      (createErrorResponse as jest.Mock).mockReturnValue({
        statusCode: 500,
        body: JSON.stringify({ error: "Internal server error" }),
      });

      const result = await handler(mockEvent as APIGatewayProxyEvent);

      expect(mockLogger.error).toHaveBeenCalledWith(
        "Unexpected error occurred",
        snsError,
        expect.any(Object)
      );

      expect(result.statusCode).toBe(500);
    });
  });

  describe("edge cases", () => {
    test("should handle empty body", async () => {
      mockEvent.body = "";
      (validateInitializeOrderData as jest.Mock).mockReturnValue({
        isValid: false,
        error: "Missing required fields",
      });
      (createErrorResponse as jest.Mock).mockReturnValue({
        statusCode: 400,
        body: JSON.stringify({ error: "Missing required fields" }),
      });

      await handler(mockEvent as APIGatewayProxyEvent);

      expect(validateInitializeOrderData).toHaveBeenCalledWith({});
    });

    test("should handle null body", async () => {
      mockEvent.body = null;
      (validateInitializeOrderData as jest.Mock).mockReturnValue({
        isValid: false,
        error: "Missing required fields",
      });
      (createErrorResponse as jest.Mock).mockReturnValue({
        statusCode: 400,
        body: JSON.stringify({ error: "Missing required fields" }),
      });

      await handler(mockEvent as APIGatewayProxyEvent);

      expect(validateInitializeOrderData).toHaveBeenCalledWith({});
    });

    test("should handle missing headers", async () => {
      mockEvent.headers = {};
      mockEvent.requestContext = { identity: {} } as any;
      (validateInitializeOrderData as jest.Mock).mockReturnValue({
        isValid: true,
      });
      (sanitizeInitializeOrderData as jest.Mock).mockReturnValue({
        cpf: "12345678901",
        email: "test@example.com",
        name: "João Silva",
        items: [{ id: "item1", quantity: 2 }],
      });
      (createSuccessResponse as jest.Mock).mockReturnValue({
        statusCode: 202,
        body: JSON.stringify({ message: "Success" }),
      });

      await handler(mockEvent as APIGatewayProxyEvent);

      expect(mockLogger.info).toHaveBeenCalledWith("Request received", {
        sourceIp: undefined,
        userAgent: undefined,
        bodyLength: expect.any(Number),
      });
    });
  });
});
