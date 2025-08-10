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
  StructuredLogger,
} from "../../shared/logger";
import { LeadData } from "../../shared/schemas/lead";
import { MessageData } from "../order/initialize-order";

const dynamoClient = new DynamoDBClient({ region: process.env.AWS_REGION || 'us-east-1' });
const docClient = DynamoDBDocumentClient.from(dynamoClient);

const LEAD_TABLE_NAME = process.env.LEAD_COLLECTION_TABLE!;

type ExistingLead = Pick<
  LeadData,
  "id" | "cpf" | "name" | "email" | "createdAt" | "updatedAt"
>;

function generateLeadId(): string {
  const timestamp = Date.now();
  const randomPart = Math.random().toString(36).substring(2, 15);
  const extraRandom = Math.random().toString(36).substring(2, 10);
  return `lead-${timestamp}-${randomPart}${extraRandom}`;
}

function normalizeCpf(cpf: string): string {
  return cpf.replace(/\D/g, '');
}

function normalizeEmail(email: string): string {
  return email.toLowerCase().trim();
}

function isValidCpfFormat(cpf: string): boolean {
  const normalizedCpf = normalizeCpf(cpf);
  return normalizedCpf.length === 11 && /^\d{11}$/.test(normalizedCpf);
}

function isValidEmailFormat(email: string): boolean {
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return emailRegex.test(email);
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
    const record = event.Records[i];
    const recordLogger = logger.withContext({
      recordIndex: i,
      messageId: record.messageId,
      receiptHandle: record.receiptHandle?.substring(0, 20) + "...",
    });

    const recordTracker = new PerformanceTracker(recordLogger, "create-lead-record");

    recordLogger.info("Processing SQS record", {
      eventSource: record.eventSource,
      eventSourceARN: record.eventSourceARN,
      messageAttributes: Object.keys(record.messageAttributes || {}),
    });

    try {
      const bodyMessage = JSON.parse(record.body);
      const messageData: MessageData = JSON.parse(bodyMessage.Message);

      if (!messageData.customerData || !messageData.customerData.cpf || !messageData.customerData.email || !messageData.customerData.name) {
        const missingFields = [];
        if (!messageData.customerData) missingFields.push('customerData');
        else {
          if (!messageData.customerData.cpf) missingFields.push('customerData.cpf');
          if (!messageData.customerData.email) missingFields.push('customerData.email');
          if (!messageData.customerData.name) missingFields.push('customerData.name');
        }
        
        recordLogger.error("Invalid lead message: missing required customer fields", null, {
          missingFields,
          messagePreview: record.body.substring(0, 200),
        });
        
        throw new Error(`Missing required customer fields: ${missingFields.join(', ')}`);
      }

      const normalizedEmail = normalizeEmail(messageData.customerData.email);
      if (!isValidEmailFormat(normalizedEmail)) {
        recordLogger.error("Invalid email format", null, {
          email: maskSensitiveData.email(messageData.customerData.email),
          messagePreview: record.body.substring(0, 200),
        });
        throw new Error(`Invalid email format: ${maskSensitiveData.email(messageData.customerData.email)}`);
      }

      const normalizedCpf = normalizeCpf(messageData.customerData.cpf);
      if (!isValidCpfFormat(normalizedCpf)) {
        recordLogger.error("Invalid CPF format", null, {
          cpf: maskSensitiveData.cpf(messageData.customerData.cpf),
          messagePreview: record.body.substring(0, 200),
        });
        throw new Error(`Invalid CPF format: ${maskSensitiveData.cpf(messageData.customerData.cpf)}`);
      }

      recordLogger.info("Lead data parsed successfully", {
        cpf: maskSensitiveData.cpf(normalizedCpf),
        email: maskSensitiveData.email(normalizedEmail),
        name: maskSensitiveData.name(messageData.customerData.name),
      });

      await createLeadInDatabase(
        {
          cpf: normalizedCpf,
          email: normalizedEmail,
          name: messageData.customerData.name,
        },
        recordLogger.withContext({ cpf: normalizedCpf, email: normalizedEmail })
      );

      recordLogger.info("Lead created successfully", {
        id: generateLeadId(),
        cpf: maskSensitiveData.cpf(normalizedCpf),
        email: maskSensitiveData.email(normalizedEmail),
        name: maskSensitiveData.name(messageData.customerData.name),
      });

      recordTracker.finish({
        leadId: generateLeadId(),
        status: "success",
      });

    } catch (error: any) {
      recordLogger.error("Error processing lead creation record", {
        sqsMessagePreview: record.body.substring(0, 200),
        errorType: error.constructor.name,
        errorMessage: error.message,
        stackTrace: error.stack?.split('\n').slice(0, 5).join('\n'),
        recordIndex: i,
        messageId: record.messageId,
      });

      recordTracker.finishWithError(error);

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
 */
async function createLeadInDatabase(
  leadData: { cpf: string; email: string; name: string },
  logger: StructuredLogger
): Promise<void> {
  const leadCreationTracker = new PerformanceTracker(logger, "lead-creation-logic");
  
  logger.info("Starting lead creation logic");

  try {
    const leadId = generateLeadId();
    
    logger.info("Starting lead validation in DynamoDB", {
      leadId,
      cpf: maskSensitiveData.cpf(leadData.cpf),
      email: maskSensitiveData.email(leadData.email),
    });

    const existingLead = await checkExistingLead(leadData.cpf, leadData.email, logger);

    if (existingLead) {
      logger.info("Lead with same email and CPF already exists, skipping insertion", {
        leadId,
        existingLeadId: existingLead.id,
        cpf: maskSensitiveData.cpf(leadData.cpf),
        email: maskSensitiveData.email(leadData.email),
      });

      leadCreationTracker.finish({
        leadId,
        action: "skipped",
        reason: "duplicate_email_cpf_combination",
      });

      return;
    }

    const newLead: LeadData = {
      id: leadId,
      cpf: leadData.cpf,
      name: leadData.name,
      email: leadData.email,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };

    const putCommand = new PutCommand({
      TableName: LEAD_TABLE_NAME,
      Item: newLead,
    });

    await docClient.send(putCommand);

    logger.info("Lead successfully inserted into DynamoDB", {
      leadId,
      cpf: maskSensitiveData.cpf(leadData.cpf),
      email: maskSensitiveData.email(leadData.email),
    });

    leadCreationTracker.finish({
      leadId,
      action: "inserted",
    });

  } catch (error: any) {
    logger.error("Failed to create lead in DynamoDB", error, {
      leadId: generateLeadId(),
      cpf: maskSensitiveData.cpf(leadData.cpf),
      email: maskSensitiveData.email(leadData.email),
    });

    leadCreationTracker.finishWithError(error);
    throw error;
  }
}

/**
 * Checks if a lead with the same email and CPF combination already exists
 */
async function checkExistingLead(
  cpf: string,
  email: string,
  logger: StructuredLogger
): Promise<ExistingLead | null> {
  const validationTracker = new PerformanceTracker(logger, "lead-validation-query");
  
  logger.info("Querying for existing leads", {
    cpf: maskSensitiveData.cpf(cpf),
    email: maskSensitiveData.email(email),
    indexName: "email-index",
  });

  try {
    const emailQueryCommand = new QueryCommand({
      TableName: LEAD_TABLE_NAME,
      IndexName: "email-index",
      KeyConditionExpression: "email = :email",
      ExpressionAttributeValues: {
        ":email": email,
      },
      ProjectionExpression: "id, cpf, email, name, createdAt, updatedAt",
    });

    const emailResult = await docClient.send(emailQueryCommand);

    logger.info("Email query completed", {
      itemsFound: emailResult.Items?.length || 0,
      email: maskSensitiveData.email(email),
    });

    if (!emailResult.Items || emailResult.Items.length === 0) {
      logger.info("No existing lead found with matching email and CPF combination", {
        cpf: maskSensitiveData.cpf(cpf),
        email: maskSensitiveData.email(email),
        emailLeadsCount: 0,
        decision: "will_create_new_lead",
      });

      validationTracker.finish({
        action: "no_duplicate_found",
        emailLeadsCount: 0,
        decision: "create_new",
      });

      return null;
    }

    const matchingLead = emailResult.Items.find((item: any) => item.cpf === cpf);

    if (matchingLead) {
      logger.info("Found existing lead with matching email and CPF", {
        existingLeadId: matchingLead.id,
        cpf: maskSensitiveData.cpf(cpf),
        email: maskSensitiveData.email(email),
      });

      validationTracker.finish({
        action: "found_duplicate",
        leadId: matchingLead.id,
        emailLeadsCount: emailResult.Items.length,
      });

      return matchingLead as ExistingLead;
    }

    logger.info("Found leads with same email but different CPFs", {
      email: maskSensitiveData.email(email),
      cpf: maskSensitiveData.cpf(cpf),
      emailLeadsCount: emailResult.Items.length,
      differentCpfCount: emailResult.Items.length,
    });

    logger.info("No existing lead found with matching email and CPF combination", {
      cpf: maskSensitiveData.cpf(cpf),
      email: maskSensitiveData.email(email),
      emailLeadsCount: emailResult.Items.length,
      decision: "will_create_new_lead",
    });

    validationTracker.finish({
      action: "no_duplicate_found",
      emailLeadsCount: emailResult.Items.length,
      decision: "create_new",
    });

    return null;

  } catch (error: any) {
    logger.error("Error querying for existing leads", error, {
      cpf: maskSensitiveData.cpf(cpf),
      email: maskSensitiveData.email(email),
    });

    validationTracker.finishWithError(error);
    throw error;
  }
}