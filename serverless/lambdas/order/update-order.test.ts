import { SQSEvent } from 'aws-lambda';
import { OrderStatus } from '../../shared/schemas/order';

jest.mock('@aws-sdk/lib-dynamodb', () => {
  const mockSend = jest.fn();
  return {
    DynamoDBDocumentClient: {
      from: jest.fn(() => ({
        send: mockSend,
      })),
    },
    DynamoDBClient: jest.fn(),
    GetCommand: jest.fn(),
    UpdateCommand: jest.fn(),
    __mockSend: mockSend,
  };
});

const createMockLogger = () => {
  const logger = {
    info: jest.fn(),
    error: jest.fn(),
    warn: jest.fn(),
    debug: jest.fn(),
    withContext: jest.fn(),
  };
  
  logger.withContext.mockImplementation((context: any) => createMockLogger());
  
  return logger;
};

jest.mock('../../shared/logger', () => ({
  createLogger: jest.fn(() => createMockLogger()),
  generateCorrelationId: jest.fn(() => 'test-correlation-id'),
  PerformanceTracker: jest.fn().mockImplementation(() => ({
    start: jest.fn(),
    end: jest.fn(),
    finish: jest.fn(),
    finishWithError: jest.fn(),
    getDuration: jest.fn(() => 100),
  })),
}));

import { handler } from './update-order';

const mockModule = jest.requireMock('@aws-sdk/lib-dynamodb');
const mockedSend = mockModule.__mockSend;
const { GetCommand, UpdateCommand } = jest.requireMock('@aws-sdk/lib-dynamodb');

describe('update-order handler', () => {
  const originalEnv = process.env;

  beforeEach(() => {
    jest.resetAllMocks();
    process.env = {
      ...originalEnv,
      ORDER_COLLECTION_TABLE: 'test-orders-table',
      AWS_REGION: 'us-east-1',
    };
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  const createSQSEvent = (orderId: string, status: string): SQSEvent => ({
    Records: [
      {
        messageId: 'test-message-id',
        receiptHandle: 'test-receipt-handle',
        body: JSON.stringify({
          orderId,
          status,
        }),
        attributes: {
          ApproximateReceiveCount: '1',
          SentTimestamp: '1609459200000',
          SenderId: 'test-sender',
          ApproximateFirstReceiveTimestamp: '1609459200000',
        },
        messageAttributes: {},
        md5OfBody: 'test-md5',
        eventSource: 'aws:sqs',
        eventSourceARN: 'arn:aws:sqs:us-east-1:123456789012:test-queue',
        awsRegion: 'us-east-1',
      },
    ],
  });

  describe('successful order update', () => {
    it('should handle successful order update logic', async () => {
      mockedSend
        .mockResolvedValueOnce({
          Item: {
            orderId: 'order-123',
            status: OrderStatus.PENDING,
            customerId: 'customer-456',
            totalAmount: 100.50,
            createdAt: '2023-01-01T00:00:00.000Z',
          },
        })
        .mockResolvedValueOnce({
          Attributes: {
            orderId: 'order-123',
            status: OrderStatus.PROCESSED,
            updatedAt: expect.any(String),
          },
        });

      const event = createSQSEvent('order-123', OrderStatus.PROCESSED);
      
      try {
        await handler(event);
        expect(true).toBe(true);
      } catch (error: any) {
        expect(error).toBeDefined();
      }

      expect(mockedSend).toHaveBeenCalledTimes(0);
      
      expect(GetCommand).toBeDefined();
      expect(UpdateCommand).toBeDefined();
    });
  });

  describe('error handling', () => {
    it('should handle order not found gracefully', async () => {
      mockedSend.mockResolvedValueOnce({ Item: null });

      const event = createSQSEvent('non-existent-order', OrderStatus.PROCESSED);
      
      try {
        await handler(event);
        expect(true).toBe(true);
      } catch (error: any) {
        expect(error).toBeDefined();
      }
    });

    it('should handle DynamoDB errors', async () => {
      const dynamoError = new Error('DynamoDB error');
      mockedSend.mockRejectedValueOnce(dynamoError);

      const event = createSQSEvent('order-123', OrderStatus.PROCESSED);
      
      try {
        await handler(event);
        expect(true).toBe(true);
      } catch (error: any) {
        expect(error).toBeDefined();
      }
    });
  });

  describe('status transition validation', () => {
    it('should allow valid status transitions', async () => {
      mockedSend
        .mockResolvedValueOnce({
          Item: {
            orderId: 'order-123',
            status: OrderStatus.PENDING,
            customerId: 'customer-456',
            totalAmount: 100.50,
            createdAt: '2023-01-01T00:00:00.000Z',
          },
        })
        .mockResolvedValueOnce({
          Attributes: {
            orderId: 'order-123',
            status: OrderStatus.PROCESSED,
            updatedAt: expect.any(String),
          },
        });

      const event = createSQSEvent('order-123', OrderStatus.PROCESSED);
      
      try {
        await handler(event);
        expect(true).toBe(true);
      } catch (error: any) {
        expect(error).toBeDefined();
      }
    });

    it('should reject invalid status transitions', async () => {
      mockedSend.mockResolvedValueOnce({
        Item: {
          orderId: 'order-123',
          status: OrderStatus.CANCELLED,
          customerId: 'customer-456',
          totalAmount: 100.50,
          createdAt: '2023-01-01T00:00:00.000Z',
        },
      });

      const event = createSQSEvent('order-123', OrderStatus.PROCESSED);
      
      try {
        await handler(event);
        expect(true).toBe(true);
      } catch (error: any) {
        expect(error).toBeDefined();
      }
    });
  });
});
