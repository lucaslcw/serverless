import { SQSEvent, SQSRecord } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  QueryCommand,
  PutCommand,
} from "@aws-sdk/lib-dynamodb";
import {
  createLogger,
  generateCorrelationId,
  maskSensitiveData,
  PerformanceTracker,
} from "../../shared/logger";

const dynamoClient = new DynamoDBClient({ region: process.env.AWS_REGION });
const docClient = DynamoDBDocumentClient.from(dynamoClient);

const LEAD_TABLE_NAME = process.env.LEAD_COLLECTION_TABLE!;

interface OrderData {
  orderId: string;
  cpf: string;
  email: string;
  name: string;
  items: Array<{
    id: string;
    quantity: number;
  }>;
  timestamp: string;
  status: string;
}

interface LeadData {
  id: string;
  leadId: string;
  customerCpf: string;
  customerEmail: string;
  customerName: string;
  orderReference: string;
  totalItems: number;
  createdAt: string;
  updatedAt: string;
  source: string;
  status: string;
}

interface ExistingLead {
  id: string;
  customerCpf: string;
  customerEmail: string;
  customerName: string;
  createdAt: string;
  updatedAt: string;
}

export const handler = async (event: SQSEvent): Promise<void> => {
  const processId = generateCorrelationId("lead");
  const logger = createLogger({
    processId,
    functionName: "create-lead",
  });

  const mainTracker = new PerformanceTracker(logger, "create-lead-batch");

  logger.info("Processing SQS batch started", {
    recordsCount: event.Records.length,
  });

  for (let i = 0; i < event.Records.length; i++) {
    const record: SQSRecord = event.Records[i];
    const recordLogger = logger.withContext({
      recordIndex: i,
      messageId: record.messageId,
      receiptHandle: record.receiptHandle.substring(0, 20) + "...",
    });

    const recordTracker = new PerformanceTracker(
      recordLogger,
      "create-lead-record"
    );

    recordLogger.info("Processing SQS record", {
      eventSource: record.eventSource,
      eventSourceARN: record.eventSourceARN,
      messageAttributes: Object.keys(record.messageAttributes || {}),
    });

    try {
      let orderData: OrderData;

      try {
        const messageBody = JSON.parse(record.body);

        if (messageBody.Type === "Notification" && messageBody.Message) {
          orderData = JSON.parse(messageBody.Message);
          recordLogger.info("SNS message detected and parsed");
        } else {
          orderData = messageBody;
          recordLogger.info("Direct SQS message detected");
        }
      } catch (parseError) {
        recordLogger.error("Failed to parse message body", parseError, {
          messageBodyPreview: record.body.substring(0, 200),
        });
        recordTracker.finishWithError(parseError);
        throw parseError;
      }

      const leadLogger = recordLogger.withContext({
        orderId: orderData.orderId,
      });

      leadLogger.info("Order data parsed successfully", {
        customerCpf: maskSensitiveData.cpf(orderData.cpf || ""),
        customerEmail: maskSensitiveData.email(orderData.email || ""),
        customerName: maskSensitiveData.name(orderData.name || ""),
        itemsCount: orderData.items ? orderData.items.length : 0,
        orderStatus: orderData.status,
        orderTimestamp: orderData.timestamp,
      });

      const processingTracker = new PerformanceTracker(
        leadLogger,
        "lead-creation-logic"
      );

      leadLogger.info("Starting lead creation logic");

      const currentTimestamp = new Date().toISOString();
      const leadId = `lead-${Date.now()}-${Math.random()
        .toString(36)
        .substr(2, 9)}`;

      const leadData: LeadData = {
        id: leadId,
        leadId: leadId,
        customerCpf: orderData.cpf,
        customerEmail: orderData.email,
        customerName: orderData.name,
        orderReference: orderData.orderId,
        totalItems: (orderData.items || []).reduce(
          (total, item) => total + item.quantity,
          0
        ),
        createdAt: currentTimestamp,
        updatedAt: currentTimestamp,
        source: "order_processing",
        status: "new",
      };

      await createLeadInDatabase(leadData, leadLogger);

      processingTracker.finish({
        leadId: leadData.leadId,
        totalItems: leadData.totalItems,
        status: leadData.status,
      });

      leadLogger.info("Lead created successfully", {
        leadId: leadData.leadId,
        customerCpf: maskSensitiveData.cpf(leadData.customerCpf),
        customerEmail: maskSensitiveData.email(leadData.customerEmail),
        customerName: maskSensitiveData.name(leadData.customerName),
        totalItems: leadData.totalItems,
        source: leadData.source,
        status: leadData.status,
      });

      recordTracker.finish({
        orderId: orderData.orderId,
        leadId: leadData.leadId,
        status: "success",
      });
    } catch (error) {
      recordLogger.error("Error processing lead creation record", error, {
        sqsMessagePreview: record.body.substring(0, 200),
      });

      recordTracker.finishWithError(error);

      throw error;
    }
  }

  mainTracker.finish({
    totalRecords: event.Records.length,
    status: "completed",
  });

  logger.info("SQS batch processing completed", {
    totalRecords: event.Records.length,
  });
};

/**
 * Creates a lead in DynamoDB with validation for existing email/CPF combinations
 * Business rule: Insert only if the combination of email+CPF doesn't exist
 * - If email exists with different CPF: insert new lead
 * - If CPF exists with different email: insert new lead
 * - If both email AND CPF exist together: skip insertion
 */
async function createLeadInDatabase(
  leadData: LeadData,
  logger: any
): Promise<void> {
  const creationTracker = new PerformanceTracker(
    logger,
    "dynamodb-lead-creation"
  );

  try {
    logger.info("Starting lead validation in DynamoDB", {
      leadId: leadData.leadId,
      customerCpf: maskSensitiveData.cpf(leadData.customerCpf),
      customerEmail: maskSensitiveData.email(leadData.customerEmail),
    });

    const existingLead = await checkExistingLead(
      leadData.customerEmail,
      leadData.customerCpf,
      logger
    );

    if (existingLead) {
      logger.info(
        "Lead with same email and CPF already exists, skipping insertion",
        {
          leadId: leadData.leadId,
          existingLeadId: existingLead.id,
          customerCpf: maskSensitiveData.cpf(leadData.customerCpf),
          customerEmail: maskSensitiveData.email(leadData.customerEmail),
        }
      );

      creationTracker.finish({
        leadId: leadData.leadId,
        action: "skipped",
        reason: "duplicate_email_cpf_combination",
      });

      return;
    }

    const putCommand = new PutCommand({
      TableName: LEAD_TABLE_NAME,
      Item: leadData,
    });

    await docClient.send(putCommand);

    logger.info("Lead successfully inserted into DynamoDB", {
      leadId: leadData.leadId,
      customerCpf: maskSensitiveData.cpf(leadData.customerCpf),
      customerEmail: maskSensitiveData.email(leadData.customerEmail),
      totalItems: leadData.totalItems,
      orderReference: leadData.orderReference,
    });

    creationTracker.finish({
      leadId: leadData.leadId,
      action: "inserted",
      totalItems: leadData.totalItems,
    });
  } catch (error) {
    logger.error("Failed to create lead in DynamoDB", error, {
      leadId: leadData.leadId,
      customerCpf: maskSensitiveData.cpf(leadData.customerCpf || ""),
      customerEmail: maskSensitiveData.email(leadData.customerEmail || ""),
    });

    creationTracker.finishWithError(error);
    throw error;
  }
}

/**
 * Checks if a lead with the same email AND CPF combination already exists
 * Returns the existing lead if found, null otherwise
 */
async function checkExistingLead(
  email: string,
  cpf: string,
  logger: any
): Promise<ExistingLead | null> {
  const validationTracker = new PerformanceTracker(
    logger,
    "lead-validation-query"
  );

  try {
    logger.info("Querying for existing leads", {
      customerCpf: maskSensitiveData.cpf(cpf),
      customerEmail: maskSensitiveData.email(email),
    });

    const emailQueryCommand = new QueryCommand({
      TableName: LEAD_TABLE_NAME,
      IndexName: "email-index",
      KeyConditionExpression: "customerEmail = :email",
      ExpressionAttributeValues: {
        ":email": email,
      },
      ProjectionExpression:
        "id, customerCpf, customerEmail, customerName, createdAt, updatedAt",
    });

    const emailResult = await docClient.send(emailQueryCommand);

    if (emailResult.Items && emailResult.Items.length > 0) {
      const matchingLead = emailResult.Items.find(
        (item) => item.customerCpf === cpf
      );

      if (matchingLead) {
        logger.info("Found existing lead with matching email and CPF", {
          existingLeadId: matchingLead.id,
          customerCpf: maskSensitiveData.cpf(cpf),
          customerEmail: maskSensitiveData.email(email),
        });

        validationTracker.finish({
          action: "found_duplicate",
          leadId: matchingLead.id,
        });

        return {
          id: matchingLead.id as string,
          customerCpf: matchingLead.customerCpf as string,
          customerEmail: matchingLead.customerEmail as string,
          customerName: matchingLead.customerName as string,
          createdAt: matchingLead.createdAt as string,
          updatedAt: matchingLead.updatedAt as string,
        };
      }
    }

    logger.info(
      "No existing lead found with matching email and CPF combination",
      {
        customerCpf: maskSensitiveData.cpf(cpf),
        customerEmail: maskSensitiveData.email(email),
        emailLeadsCount: emailResult.Items?.length || 0,
      }
    );

    validationTracker.finish({
      action: "no_duplicate_found",
      emailLeadsCount: emailResult.Items?.length || 0,
    });

    return null;
  } catch (error) {
    logger.error("Error querying for existing leads", error, {
      customerCpf: maskSensitiveData.cpf(cpf || ""),
      customerEmail: maskSensitiveData.email(email || ""),
    });

    validationTracker.finishWithError(error);
    throw error;
  }
}
