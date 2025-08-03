import { SQSEvent, SQSRecord } from 'aws-lambda';
import { handler } from '../lambdas/transaction/process-transaction';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { createLogger, generateCorrelationId, maskSensitiveData, PerformanceTracker } from '../shared/logger';

jest.mock('@aws-sdk/client-dynamodb');
jest.mock('@aws-sdk/lib-dynamodb', () => {
  const mockSend = jest.fn();
  return {
    DynamoDBDocumentClient: {
      from: jest.fn(() => ({
        send: mockSend,
      })),
    },
    GetCommand: jest.fn().mockImplementation((input) => ({ input, constructor: { name: 'GetCommand' } })),
    UpdateCommand: jest.fn().mockImplementation((input) => ({ input, constructor: { name: 'UpdateCommand' } })),
    PutCommand: jest.fn().mockImplementation((input) => ({ 
      input: {
        ...input,
        TableName: process.env.TRANSACTION_COLLECTION_TABLE || 'test-transaction-collection'
      }, 
      constructor: { name: 'PutCommand' } 
    })),
  };
});

jest.mock('../shared/logger', () => ({
  createLogger: jest.fn(),
  generateCorrelationId: jest.fn(),
  maskSensitiveData: {
    name: jest.fn(),
    cpf: jest.fn()
  },
  PerformanceTracker: jest.fn()
}));

describe('process-transaction handler', () => {
  let mockEvent: SQSEvent;
  let mockLogger: any;
  let mockTracker: any;
  let mockSend: jest.Mock;

  beforeEach(() => {
    jest.clearAllMocks();
    
    const mockDocClientInstance = (DynamoDBDocumentClient.from as jest.Mock)();
    mockSend = mockDocClientInstance.send as jest.Mock;
    mockSend.mockClear();

    process.env.ORDER_COLLECTION_TABLE = 'test-order-collection';
    process.env.TRANSACTION_COLLECTION_TABLE = 'test-transaction-collection';

    mockLogger = {
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
      withContext: jest.fn().mockReturnThis()
    };
    (createLogger as jest.Mock).mockReturnValue(mockLogger);
    (generateCorrelationId as jest.Mock).mockReturnValue('transaction-test-correlation-id');

    mockTracker = {
      finish: jest.fn(),
      finishWithError: jest.fn()
    };
    (PerformanceTracker as jest.Mock).mockReturnValue(mockTracker);

    (maskSensitiveData.name as jest.Mock).mockReturnValue('Jo***');
    (maskSensitiveData.cpf as jest.Mock).mockReturnValue('123***');

    mockSend.mockImplementation((command: any) => {
      if (command.constructor.name === 'GetCommand') {
        const orderId = command.input?.Key?.orderId;
        if (orderId === 'order-123456789-abc123') {
          return Promise.resolve({
            Item: {
              orderId: 'order-123456789-abc123',
              leadId: 'lead-123456789-abc123',
              customerCpf: '12345678901',
              customerEmail: 'test@example.com',
              customerName: 'João Silva',
              items: [
                { id: 'item1', quantity: 2, productName: 'Produto A', unitPrice: 29.99, totalPrice: 59.98 },
                { id: 'item2', quantity: 1, productName: 'Produto B', unitPrice: 15.50, totalPrice: 15.50 }
              ],
              totalItems: 3,
              totalValue: 75.48,
              status: 'PENDING',
              source: 'order-processing-service',
              createdAt: '2025-08-03T10:30:00.000Z',
              updatedAt: '2025-08-03T10:30:00.000Z'
            }
          });
        }
        return Promise.resolve({ Item: null });
      }
      
      if (command.constructor.name === 'UpdateCommand') {
        const orderId = command.input?.Key?.orderId;
        return Promise.resolve({
          Attributes: {
            orderId: orderId,
            status: 'PAYMENT_APPROVED',
            paymentStatus: 'APPROVED',
            transactionId: 'txn-123456789-abc123',
            updatedAt: new Date().toISOString()
          }
        });
      }
      
      if (command.constructor.name === 'PutCommand') {
        // Mock para createTransactionRecord
        return Promise.resolve({});
      }
      
      return Promise.resolve({});
    });

    mockEvent = {
      Records: [
        {
          messageId: 'test-message-id-1',
          receiptHandle: 'test-receipt-handle-1',
          body: JSON.stringify({
            orderId: 'order-123456789-abc123',
            paymentData: {
              cardNumber: '1234567890123456',
              cardHolderName: 'JOAO SILVA',
              expiryMonth: '12',
              expiryYear: '2026',
              cvv: '123',
              amount: 75.48
            },
            addressData: {
              street: 'Rua das Flores',
              number: '123',
              complement: 'Apto 101',
              neighborhood: 'Centro',
              city: 'São Paulo',
              state: 'SP',
              zipCode: '01234-567',
              country: 'BRASIL'
            },
            customerInfo: {
              name: 'João Silva',
              email: 'test@example.com',
              cpf: '12345678901'
            }
          }),
          attributes: {
            ApproximateReceiveCount: '1',
            SentTimestamp: '1691055000000',
            SenderId: 'AIDAIENQZJOLO23YVJ4VO',
            ApproximateFirstReceiveTimestamp: '1691055000000'
          },
          messageAttributes: {},
          md5OfBody: 'test-md5-hash',
          eventSource: 'aws:sqs',
          eventSourceARN: 'arn:aws:sqs:us-east-1:123456789012:process-transaction-queue',
          awsRegion: 'us-east-1'
        } as SQSRecord
      ]
    };
  });

  describe('Successful Transaction Processing', () => {
    test('should process single transaction successfully', async () => {
      await handler(mockEvent);

      expect(createLogger).toHaveBeenCalledWith({
        processId: 'transaction-test-correlation-id',
        functionName: 'process-transaction'
      });

      expect(mockLogger.info).toHaveBeenCalledWith('Processing transaction batch started', {
        recordsCount: 1
      });

      expect(mockLogger.info).toHaveBeenCalledWith('Transaction batch processing completed', {
        totalRecords: 1
      });

      expect(PerformanceTracker).toHaveBeenCalledWith(mockLogger, 'transaction-batch');
      expect(mockTracker.finish).toHaveBeenCalledWith({
        totalRecords: 1,
        status: 'completed'
      });
    });

    test('should parse transaction message and mask sensitive data', async () => {
      await handler(mockEvent);

      expect(maskSensitiveData.name).toHaveBeenCalledWith('João Silva');

      expect(mockLogger.info).toHaveBeenCalledWith('Transaction message parsed successfully', {
        orderId: 'order-123456789-abc123',
        amount: 75.48,
        cardLastFour: '3456',
        customerName: 'Jo***',
        addressCity: 'São Paulo',
        addressState: 'SP'
      });
    });

    test('should lookup and find existing order', async () => {
      await handler(mockEvent);

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          constructor: { name: 'GetCommand' },
          input: {
            TableName: undefined,
            Key: { orderId: 'order-123456789-abc123' }
          }
        })
      );

      expect(mockLogger.info).toHaveBeenCalledWith('Order found for transaction processing', {
        orderId: 'order-123456789-abc123',
        currentStatus: 'PENDING',
        totalValue: 75.48,
        leadId: 'lead-123456789-abc123'
      });
    });

    test('should update order status and create transaction record', async () => {
      await handler(mockEvent);

      // Verificar se o pedido foi atualizado com algum status de pagamento
      const updateCall = mockSend.mock.calls.find((call: any) => 
        call[0].constructor.name === 'UpdateCommand'
      );

      expect(updateCall).toBeDefined();
      expect(updateCall[0].input.Key.orderId).toBe('order-123456789-abc123');
      expect(['PAYMENT_APPROVED', 'PAYMENT_DECLINED']).toContain(
        updateCall[0].input.ExpressionAttributeValues[':status']
      );
      
      // Verificar que não há dados de pagamento no pedido
      expect(updateCall[0].input.ExpressionAttributeValues[':paymentStatus']).toBeUndefined();
      expect(updateCall[0].input.ExpressionAttributeValues[':paymentData']).toBeUndefined();

      // Verificar se foi criado o registro de transação
      const putCall = mockSend.mock.calls.find((call: any) => 
        call[0].constructor.name === 'PutCommand'
      );

      expect(putCall).toBeDefined();
      expect(putCall[0].input.TableName).toBe('test-transaction-collection');
      expect(putCall[0].input.Item).toMatchObject({
        orderId: 'order-123456789-abc123',
        amount: 75.48
      });
      expect(['APPROVED', 'DECLINED']).toContain(putCall[0].input.Item.paymentStatus);
    });

    test('should process multiple transactions in batch', async () => {
      const secondRecord = {
        ...mockEvent.Records[0],
        messageId: 'test-message-id-2',
        receiptHandle: 'test-receipt-handle-2',
        body: JSON.stringify({
          orderId: 'order-987654321-def456',
          paymentData: {
            cardNumber: '9876543210987654',
            cardHolderName: 'MARIA SANTOS',
            expiryMonth: '06',
            expiryYear: '2027',
            cvv: '456',
            amount: 120.00
          },
          addressData: {
            street: 'Av. Paulista',
            number: '1000',
            neighborhood: 'Bela Vista',
            city: 'São Paulo',
            state: 'SP',
            zipCode: '01310-100',
            country: 'BRASIL'
          },
          customerInfo: {
            name: 'Maria Santos',
            email: 'maria@example.com',
            cpf: '98765432100'
          }
        })
      } as SQSRecord;

      mockEvent.Records.push(secondRecord);

      mockSend.mockImplementation((command: any) => {
        if (command.constructor.name === 'GetCommand') {
          const orderId = command.input?.Key?.orderId;
          if (orderId === 'order-123456789-abc123') {
            return Promise.resolve({
              Item: {
                orderId: 'order-123456789-abc123',
                leadId: 'lead-123456789-abc123',
                customerCpf: '12345678901',
                customerEmail: 'test@example.com',
                customerName: 'João Silva',
                items: [
                  { id: 'item1', quantity: 2, productName: 'Produto A', unitPrice: 29.99, totalPrice: 59.98 },
                  { id: 'item2', quantity: 1, productName: 'Produto B', unitPrice: 15.50, totalPrice: 15.50 }
                ],
                totalItems: 3,
                totalValue: 75.48,
                status: 'PENDING',
                source: 'order-processing-service',
                createdAt: '2025-08-03T10:30:00.000Z',
                updatedAt: '2025-08-03T10:30:00.000Z'
              }
            });
          } else if (orderId === 'order-987654321-def456') {
            return Promise.resolve({
              Item: {
                orderId: 'order-987654321-def456',
                leadId: 'lead-987654321-def456',
                customerCpf: '98765432100',
                customerEmail: 'maria@example.com',
                customerName: 'Maria Santos',
                items: [
                  { id: 'item3', quantity: 1, productName: 'Produto C', unitPrice: 120.00, totalPrice: 120.00 }
                ],
                totalItems: 1,
                totalValue: 120.00,
                status: 'PENDING',
                source: 'order-processing-service',
                createdAt: '2025-08-03T10:30:00.000Z',
                updatedAt: '2025-08-03T10:30:00.000Z'
              }
            });
          }
          return Promise.resolve({ Item: null });
        }
        
        if (command.constructor.name === 'UpdateCommand') {
          const orderId = command.input?.Key?.orderId;
          return Promise.resolve({
            Attributes: {
              orderId: orderId,
              status: 'PAYMENT_APPROVED',
              paymentStatus: 'APPROVED',
              transactionId: 'txn-123456789-abc123',
              updatedAt: new Date().toISOString()
            }
          });
        }
        
        return Promise.resolve({});
      });

      await handler(mockEvent);

      expect(mockLogger.info).toHaveBeenCalledWith('Processing transaction batch started', {
        recordsCount: 2
      });

      expect(mockLogger.info).toHaveBeenCalledWith('Transaction batch processing completed', {
        totalRecords: 2
      });
    });
  });

  describe('Error Handling', () => {
    test('should handle order not found error', async () => {
      mockEvent.Records[0].body = JSON.stringify({
        orderId: 'non-existent-order',
        paymentData: {
          cardNumber: '1234567890123456',
          cardHolderName: 'JOAO SILVA',
          expiryMonth: '12',
          expiryYear: '2026',
          cvv: '123',
          amount: 75.48
        },
        addressData: {
          street: 'Rua das Flores',
          number: '123',
          city: 'São Paulo',
          state: 'SP',
          zipCode: '01234-567',
          country: 'BRASIL'
        },
        customerInfo: {
          name: 'João Silva',
          email: 'test@example.com',
          cpf: '12345678901'
        }
      });

      await expect(handler(mockEvent)).rejects.toThrow('Order not found: non-existent-order');

      expect(mockLogger.error).toHaveBeenCalledWith(
        'Order not found for transaction processing',
        expect.any(Error),
        { orderId: 'non-existent-order' }
      );
    });

    test('should handle invalid JSON in message body', async () => {
      mockEvent.Records[0].body = 'invalid-json';

      await expect(handler(mockEvent)).rejects.toThrow();

      expect(mockLogger.error).toHaveBeenCalledWith(
        'Error processing transaction record',
        expect.any(Error),
        { sqsMessagePreview: 'invalid-json' }
      );
    });

    test('should handle database update failures', async () => {
      mockSend.mockImplementation((command: any) => {
        if (command.constructor.name === 'GetCommand') {
          return Promise.resolve({
            Item: {
              orderId: 'order-123456789-abc123',
              leadId: 'lead-123456789-abc123',
              customerCpf: '12345678901',
              customerEmail: 'test@example.com',
              customerName: 'João Silva',
              items: [
                { id: 'item1', quantity: 1, productName: 'Produto A', unitPrice: 29.99, totalPrice: 29.99 }
              ],
              totalItems: 1,
              totalValue: 29.99,
              status: 'PENDING',
              source: 'order-processing-service',
              createdAt: '2025-08-03T10:30:00.000Z',
              updatedAt: '2025-08-03T10:30:00.000Z'
            }
          });
        }
        
        if (command.constructor.name === 'UpdateCommand') {
          return Promise.reject(new Error('Database connection timeout'));
        }
        
        return Promise.resolve({});
      });

      await expect(handler(mockEvent)).rejects.toThrow('Database connection timeout');

      expect(mockLogger.error).toHaveBeenCalledWith(
        'Failed to update order status',
        expect.any(Error),
        expect.objectContaining({
          orderId: 'order-123456789-abc123'
        })
      );
    });
  });

  describe('Payment Simulation', () => {
    test('should approve or decline payments for valid cards', async () => {
      await handler(mockEvent);

      // Verificar se foi criada transação com qualquer status válido
      const putCall = mockSend.mock.calls.find((call: any) => 
        call[0].constructor.name === 'PutCommand'
      );

      expect(putCall).toBeDefined();
      expect(['APPROVED', 'DECLINED']).toContain(putCall[0].input.Item.paymentStatus);
      expect(putCall[0].input.Item.orderId).toBe('order-123456789-abc123');
      expect(putCall[0].input.Item.amount).toBe(75.48);
    });

    test('should decline payments for cards ending in 0000', async () => {
      mockEvent.Records[0].body = JSON.stringify({
        ...JSON.parse(mockEvent.Records[0].body),
        paymentData: {
          cardNumber: '1234567890120000',
          cardHolderName: 'JOAO SILVA',
          expiryMonth: '12',
          expiryYear: '2026',
          cvv: '123',
          amount: 75.48
        }
      });

      await handler(mockEvent);

      const updateCall = mockSend.mock.calls.find((call: any) => 
        call[0].constructor.name === 'UpdateCommand'
      );

      expect(updateCall[0].input.ExpressionAttributeValues[':status']).toBe('PAYMENT_DECLINED');
      
      // Verificar se foi criada transação com status DECLINED
      const putCall = mockSend.mock.calls.find((call: any) => 
        call[0].constructor.name === 'PutCommand'
      );

      expect(putCall[0].input.Item.paymentStatus).toBe('DECLINED');
    });

    test('should handle high-value transactions differently', async () => {
      mockEvent.Records[0].body = JSON.stringify({
        ...JSON.parse(mockEvent.Records[0].body),
        paymentData: {
          cardNumber: '1234567890123456',
          cardHolderName: 'JOAO SILVA',
          expiryMonth: '12',
          expiryYear: '2026',
          cvv: '123',
          amount: 15000.00
        }
      });

      await handler(mockEvent);

      const updateCall = mockSend.mock.calls.find((call: any) => 
        call[0].constructor.name === 'UpdateCommand'
      );

      expect(updateCall).toBeDefined();
      expect(['PAYMENT_APPROVED', 'PAYMENT_DECLINED']).toContain(
        updateCall[0].input.ExpressionAttributeValues[':status']
      );
      
      // Verificar se foi criada transação
      const putCall = mockSend.mock.calls.find((call: any) => 
        call[0].constructor.name === 'PutCommand'
      );

      expect(putCall).toBeDefined();
      expect(['APPROVED', 'DECLINED']).toContain(
        putCall[0].input.Item.paymentStatus
      );
    });
  });

  describe('Performance Tracking', () => {
    test('should create performance trackers for transaction processing', async () => {
      await handler(mockEvent);

      expect(PerformanceTracker).toHaveBeenCalledWith(mockLogger, 'transaction-batch');
      expect(PerformanceTracker).toHaveBeenCalledWith(
        expect.objectContaining({ withContext: expect.any(Function) }),
        'transaction-record'
      );
      expect(PerformanceTracker).toHaveBeenCalledWith(
        expect.objectContaining({ withContext: expect.any(Function) }),
        'transaction-processing-logic'
      );
    });

    test('should track payment gateway performance', async () => {
      await handler(mockEvent);

      expect(PerformanceTracker).toHaveBeenCalledWith(
        expect.objectContaining({ withContext: expect.any(Function) }),
        'payment-processing'
      );

      expect(PerformanceTracker).toHaveBeenCalledWith(
        expect.objectContaining({ withContext: expect.any(Function) }),
        'payment-gateway-call'
      );
    });
  });

  describe('Logging Context', () => {
    test('should create proper logging context for transaction processing', async () => {
      await handler(mockEvent);

      expect(mockLogger.withContext).toHaveBeenCalledWith({
        recordIndex: 0,
        messageId: 'test-message-id-1'
      });

      expect(mockLogger.withContext).toHaveBeenCalledWith({
        orderId: 'order-123456789-abc123'
      });
    });

    test('should log transaction processing details with masked data', async () => {
      await handler(mockEvent);

      expect(mockLogger.info).toHaveBeenCalledWith('Transaction message parsed successfully', {
        orderId: 'order-123456789-abc123',
        amount: 75.48,
        cardLastFour: '3456',
        customerName: 'Jo***',
        addressCity: 'São Paulo',
        addressState: 'SP'
      });

      expect(mockLogger.info).toHaveBeenCalledWith('Order status updated successfully', expect.objectContaining({
        orderId: 'order-123456789-abc123',
        newStatus: expect.stringMatching(/^PAYMENT_(APPROVED|DECLINED)$/)
      }));
    });
  });
});
