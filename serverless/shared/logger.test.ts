import {
  createLogger,
  generateCorrelationId,
  maskSensitiveData,
  PerformanceTracker,
  withPerformanceLogging,
  LogContext,
} from "../shared/logger";

const mockConsoleLog = jest.spyOn(console, "log").mockImplementation();
const mockConsoleWarn = jest.spyOn(console, "warn").mockImplementation();
const mockConsoleError = jest.spyOn(console, "error").mockImplementation();

describe("Logger Module", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    delete process.env.LOG_LEVEL;
  });

  afterAll(() => {
    mockConsoleLog.mockRestore();
    mockConsoleWarn.mockRestore();
    mockConsoleError.mockRestore();
  });

  describe("generateCorrelationId", () => {
    test("should generate correlation ID with default prefix", () => {
      const correlationId = generateCorrelationId();

      expect(correlationId).toMatch(/^req-\d+-[a-z0-9]{6}$/);
      expect(correlationId).toContain("req-");
    });

    test("should generate correlation ID with custom prefix", () => {
      const correlationId = generateCorrelationId("test");

      expect(correlationId).toMatch(/^test-\d+-[a-z0-9]{6}$/);
      expect(correlationId).toContain("test-");
    });

    test("should generate unique IDs", () => {
      const id1 = generateCorrelationId();
      const id2 = generateCorrelationId();

      expect(id1).not.toBe(id2);
    });
  });

  describe("maskSensitiveData", () => {
    describe("cpf masking", () => {
      test("should mask valid CPF", () => {
        const result = maskSensitiveData.cpf("12345678901");
        expect(result).toBe("123***");
      });

      test("should handle short CPF", () => {
        const result = maskSensitiveData.cpf("12");
        expect(result).toBe("***");
      });

      test("should handle empty CPF", () => {
        const result = maskSensitiveData.cpf("");
        expect(result).toBe("***");
      });

      test("should handle null/undefined CPF", () => {
        expect(maskSensitiveData.cpf(null as any)).toBe("***");
        expect(maskSensitiveData.cpf(undefined as any)).toBe("***");
      });
    });

    describe("email masking", () => {
      test("should mask valid email", () => {
        const result = maskSensitiveData.email("test@example.com");
        expect(result).toBe("tes***@example.com");
      });

      test("should mask short email", () => {
        const result = maskSensitiveData.email("ab@example.com");
        expect(result).toBe("***@example.com");
      });

      test("should handle invalid email format", () => {
        const result = maskSensitiveData.email("invalid-email");
        expect(result).toBe("***");
      });

      test("should handle empty email", () => {
        const result = maskSensitiveData.email("");
        expect(result).toBe("***");
      });

      test("should handle email with long local part", () => {
        const result = maskSensitiveData.email("verylongemail@example.com");
        expect(result).toBe("ver***@example.com");
      });
    });

    describe("credit card masking", () => {
      test("should mask valid credit card", () => {
        const result = maskSensitiveData.creditCard("1234567890123456");
        expect(result).toBe("****-****-****-3456");
      });

      test("should handle short credit card", () => {
        const result = maskSensitiveData.creditCard("123");
        expect(result).toBe("***");
      });

      test("should handle empty credit card", () => {
        const result = maskSensitiveData.creditCard("");
        expect(result).toBe("***");
      });
    });
  });

  describe("StructuredLogger", () => {
    test("should create logger with default context", () => {
      const logger = createLogger();

      logger.info("test message");

      expect(mockConsoleLog).toHaveBeenCalledWith(
        expect.stringContaining('"message":"test message"')
      );
      expect(mockConsoleLog).toHaveBeenCalledWith(
        expect.stringContaining('"level":"INFO"')
      );
    });

    test("should create logger with custom context", () => {
      const context: LogContext = {
        correlationId: "test-123",
        functionName: "test-function",
      };

      const logger = createLogger(context);
      logger.info("test message");

      const logCall = mockConsoleLog.mock.calls[0][0];
      const logData = JSON.parse(logCall);

      expect(logData.context.correlationId).toBe("test-123");
      expect(logData.context.functionName).toBe("test-function");
    });

    test("should log info messages correctly", () => {
      const logger = createLogger();
      const testData = { key: "value" };

      logger.info("info message", testData);

      const logCall = mockConsoleLog.mock.calls[0][0];
      const logData = JSON.parse(logCall);

      expect(logData.level).toBe("INFO");
      expect(logData.message).toBe("info message");
      expect(logData.key).toBe("value");
      expect(logData.timestamp).toMatch(
        /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/
      );
    });

    test("should log warning messages correctly", () => {
      const logger = createLogger();

      logger.warn("warning message", { warning: true });

      const logCall = mockConsoleWarn.mock.calls[0][0];
      const logData = JSON.parse(logCall);

      expect(logData.level).toBe("WARN");
      expect(logData.message).toBe("warning message");
      expect(logData.warning).toBe(true);
    });

    test("should log error messages with Error object", () => {
      const logger = createLogger();
      const testError = new Error("Test error message");
      testError.stack = "Error stack trace";

      logger.error("error occurred", testError, { additional: "data" });

      const logCall = mockConsoleError.mock.calls[0][0];
      const logData = JSON.parse(logCall);

      expect(logData.level).toBe("ERROR");
      expect(logData.message).toBe("error occurred");
      expect(logData.error.name).toBe("Error");
      expect(logData.error.message).toBe("Test error message");
      expect(logData.error.stack).toBe("Error stack trace");
      expect(logData.additional).toBe("data");
    });

    test("should log error messages with non-Error object", () => {
      const logger = createLogger();
      const testError = "string error";

      logger.error("error occurred", testError);

      const logCall = mockConsoleError.mock.calls[0][0];
      const logData = JSON.parse(logCall);

      expect(logData.error).toBe("string error");
    });

    test("should log debug messages when LOG_LEVEL is DEBUG", () => {
      process.env.LOG_LEVEL = "DEBUG";
      const logger = createLogger();

      logger.debug("debug message", { debug: true });

      expect(mockConsoleLog).toHaveBeenCalled();
      const logCall = mockConsoleLog.mock.calls[0][0];
      const logData = JSON.parse(logCall);

      expect(logData.level).toBe("DEBUG");
      expect(logData.message).toBe("debug message");
    });

    test("should not log debug messages when LOG_LEVEL is not DEBUG", () => {
      const logger = createLogger();

      logger.debug("debug message");

      expect(mockConsoleLog).not.toHaveBeenCalled();
    });

    test("should create new logger with additional context", () => {
      const logger = createLogger({ correlationId: "test-123" });
      const newLogger = logger.withContext({ orderId: "order-456" });

      newLogger.info("test message");

      const logCall = mockConsoleLog.mock.calls[0][0];
      const logData = JSON.parse(logCall);

      expect(logData.context.correlationId).toBe("test-123");
      expect(logData.context.orderId).toBe("order-456");
    });

    test("should override context properties", () => {
      const logger = createLogger({ functionName: "original" });
      const newLogger = logger.withContext({ functionName: "updated" });

      newLogger.info("test message");

      const logCall = mockConsoleLog.mock.calls[0][0];
      const logData = JSON.parse(logCall);

      expect(logData.context.functionName).toBe("updated");
    });
  });

  describe("PerformanceTracker", () => {
    let logger: any;

    beforeEach(() => {
      logger = createLogger({ correlationId: "test-123" });
    });

    test("should log start message on creation", () => {
      new PerformanceTracker(logger, "test-operation");

      expect(mockConsoleLog).toHaveBeenCalledWith(
        expect.stringContaining('"message":"test-operation started"')
      );
    });

    test("should log completion with duration", (done) => {
      const tracker = new PerformanceTracker(logger, "test-operation");

      mockConsoleLog.mockClear();

      setTimeout(() => {
        tracker.finish({ result: "success" });

        const logCall = mockConsoleLog.mock.calls[0][0];
        const logData = JSON.parse(logCall);

        expect(logData.message).toBe("test-operation completed");
        expect(logData.duration).toMatch(/^\d+ms$/);
        expect(logData.result).toBe("success");
        expect(parseInt(logData.duration)).toBeGreaterThanOrEqual(10);

        done();
      }, 10);
    });

    test("should log error with duration", (done) => {
      const tracker = new PerformanceTracker(logger, "test-operation");
      const testError = new Error("Test error");

      mockConsoleLog.mockClear();

      setTimeout(() => {
        tracker.finishWithError(testError, { context: "error-context" });

        const logCall = mockConsoleError.mock.calls[0][0];
        const logData = JSON.parse(logCall);

        expect(logData.message).toBe("test-operation failed");
        expect(logData.duration).toMatch(/^\d+ms$/);
        expect(logData.error.message).toBe("Test error");
        expect(logData.context).toBe("error-context");

        done();
      }, 10);
    });
  });

  describe("withPerformanceLogging decorator", () => {
    test("should create performance tracking decorator function", () => {
      const decorator = withPerformanceLogging("test-operation");

      expect(typeof decorator).toBe("function");
    });

    test("should track performance of decorated function", async () => {
      const testFunction = async (value: string): Promise<string> => {
        const logger = createLogger({ functionName: "testFunction" });
        const tracker = new PerformanceTracker(logger, "test-operation");

        try {
          await new Promise((resolve) => setTimeout(resolve, 10));
          const result = `processed: ${value}`;
          tracker.finish();
          return result;
        } catch (error) {
          tracker.finishWithError(error);
          throw error;
        }
      };

      const result = await testFunction("test-value");

      expect(result).toBe("processed: test-value");

      expect(mockConsoleLog).toHaveBeenCalledWith(
        expect.stringContaining('"message":"test-operation started"')
      );
      expect(mockConsoleLog).toHaveBeenCalledWith(
        expect.stringContaining('"message":"test-operation completed"')
      );
    });

    test("should track performance of failing function", async () => {
      const failingFunction = async (): Promise<void> => {
        const logger = createLogger({ functionName: "failingFunction" });
        const tracker = new PerformanceTracker(logger, "failing-operation");

        try {
          throw new Error("Method failed");
        } catch (error) {
          tracker.finishWithError(error);
          throw error;
        }
      };

      await expect(failingFunction()).rejects.toThrow("Method failed");

      expect(mockConsoleLog).toHaveBeenCalledWith(
        expect.stringContaining('"message":"failing-operation started"')
      );
      expect(mockConsoleError).toHaveBeenCalledWith(
        expect.stringContaining('"message":"failing-operation failed"')
      );
    });

    test("should preserve function arguments and return value", async () => {
      const functionWithArgs = async (
        a: number,
        b: string
      ): Promise<string> => {
        const logger = createLogger({ functionName: "functionWithArgs" });
        const tracker = new PerformanceTracker(logger, "method-with-args");

        try {
          const result = `${a}-${b}`;
          tracker.finish({ args: { a, b } });
          return result;
        } catch (error) {
          tracker.finishWithError(error);
          throw error;
        }
      };

      const result = await functionWithArgs(42, "test");

      expect(result).toBe("42-test");

      const logCall = mockConsoleLog.mock.calls.find((call) =>
        call[0].includes("method-with-args completed")
      );
      expect(logCall).toBeDefined();

      const logData = JSON.parse(logCall![0]);
      expect(logData.args.a).toBe(42);
      expect(logData.args.b).toBe("test");
    });
  });

  describe("integration scenarios", () => {
    test("should work with hierarchical logging context", () => {
      const mainLogger = createLogger({ correlationId: "main-123" });
      const recordLogger = mainLogger.withContext({ recordIndex: 0 });
      const orderLogger = recordLogger.withContext({ orderId: "order-456" });

      orderLogger.info("processing order", { status: "processing" });

      const logCall = mockConsoleLog.mock.calls[0][0];
      const logData = JSON.parse(logCall);

      expect(logData.context.correlationId).toBe("main-123");
      expect(logData.context.recordIndex).toBe(0);
      expect(logData.context.orderId).toBe("order-456");
      expect(logData.status).toBe("processing");
    });

    test("should work with performance tracking and sensitive data masking", () => {
      const logger = createLogger({ correlationId: "test-123" });
      const tracker = new PerformanceTracker(logger, "order-processing");

      mockConsoleLog.mockClear();

      const sensitiveData = {
        cpf: maskSensitiveData.cpf("12345678901"),
        email: maskSensitiveData.email("user@example.com"),
        itemCount: 5,
      };

      tracker.finish(sensitiveData);

      const logCall = mockConsoleLog.mock.calls[0][0];
      const logData = JSON.parse(logCall);

      expect(logData.cpf).toBe("123***");
      expect(logData.email).toBe("use***@example.com");
      expect(logData.itemCount).toBe(5);
      expect(logData.duration).toMatch(/^\d+ms$/);
    });

    test("should handle complex nested data structures", () => {
      const logger = createLogger();

      const complexData = {
        order: {
          id: "order-123",
          customer: {
            cpf: maskSensitiveData.cpf("12345678901"),
            email: maskSensitiveData.email("customer@example.com"),
          },
          items: [
            { id: "item1", quantity: 2 },
            { id: "item2", quantity: 1 },
          ],
        },
        metadata: {
          timestamp: new Date().toISOString(),
          source: "api",
        },
      };

      logger.info("complex order processed", complexData);

      const logCall = mockConsoleLog.mock.calls[0][0];
      const logData = JSON.parse(logCall);

      expect(logData.order.customer.cpf).toBe("123***");
      expect(logData.order.customer.email).toBe("cus***@example.com");
      expect(logData.order.items).toHaveLength(2);
      expect(logData.metadata.source).toBe("api");
    });
  });
});
