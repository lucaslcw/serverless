export interface LogContext {
  correlationId?: string;
  orderId?: string;
  userId?: string;
  functionName?: string;
  [key: string]: any;
}

export interface LogMetadata {
  timestamp: string;
  level: "INFO" | "WARN" | "ERROR" | "DEBUG";
  context?: LogContext;
  [key: string]: any;
}

export const generateCorrelationId = (prefix: string = "req"): string => {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).substr(2, 6)}`;
};

export const maskSensitiveData = {
  cpf: (cpf: string): string => {
    if (!cpf || cpf.length < 3) return "***";
    return cpf.substring(0, 3) + "***";
  },

  email: (email: string): string => {
    if (!email || !email.includes("@")) return "***";
    const [local, domain] = email.split("@");
    const maskedLocal =
      local.length > 3 ? local.substring(0, 3) + "***" : "***";
    return `${maskedLocal}@${domain}`;
  },

  name: (name: string): string => {
    if (!name || name.length < 2) return "***";
    const names = name.trim().split(" ");
    if (names.length === 1) {
      return names[0].substring(0, 2) + "***";
    }
    return (
      names[0].substring(0, 2) +
      "*** " +
      names[names.length - 1].substring(0, 1) +
      "***"
    );
  },

  creditCard: (cardNumber: string): string => {
    if (!cardNumber || cardNumber.length < 4) return "***";
    return "****-****-****-" + cardNumber.slice(-4);
  },
};

class StructuredLogger {
  private context: LogContext;

  constructor(context: LogContext = {}) {
    this.context = context;
  }

  private createLogEntry(
    level: LogMetadata["level"],
    message: string,
    data?: any
  ): LogMetadata {
    return {
      timestamp: new Date().toISOString(),
      level,
      message,
      context: this.context,
      ...data,
    };
  }

  info(message: string, data?: any): void {
    console.log(JSON.stringify(this.createLogEntry("INFO", message, data)));
  }

  warn(message: string, data?: any): void {
    console.warn(JSON.stringify(this.createLogEntry("WARN", message, data)));
  }

  error(message: string, error?: Error | any, data?: any): void {
    const errorData = {
      error:
        error instanceof Error
          ? {
              name: error.name,
              message: error.message,
              stack: error.stack,
            }
          : error,
      ...data,
    };
    console.error(
      JSON.stringify(this.createLogEntry("ERROR", message, errorData))
    );
  }

  debug(message: string, data?: any): void {
    if (process.env.LOG_LEVEL === "DEBUG") {
      console.log(JSON.stringify(this.createLogEntry("DEBUG", message, data)));
    }
  }

  withContext(additionalContext: LogContext): StructuredLogger {
    return new StructuredLogger({
      ...this.context,
      ...additionalContext,
    });
  }
}

export const createLogger = (context: LogContext = {}): StructuredLogger => {
  return new StructuredLogger(context);
};

export class PerformanceTracker {
  private startTime: number;
  private logger: StructuredLogger;
  private operation: string;

  constructor(logger: StructuredLogger, operation: string) {
    this.logger = logger;
    this.operation = operation;
    this.startTime = Date.now();

    this.logger.info(`${operation} started`);
  }

  finish(data?: any): void {
    const duration = Date.now() - this.startTime;
    this.logger.info(`${this.operation} completed`, {
      duration: `${duration}ms`,
      ...data,
    });
  }

  finishWithError(error: Error | any, data?: any): void {
    const duration = Date.now() - this.startTime;
    this.logger.error(`${this.operation} failed`, error, {
      duration: `${duration}ms`,
      ...data,
    });
  }
}

export const withPerformanceLogging = (operation: string) => {
  return (
    target: any,
    propertyName: string,
    descriptor: PropertyDescriptor
  ) => {
    const method = descriptor.value;

    descriptor.value = async function (...args: any[]) {
      const logger = createLogger({ functionName: propertyName });
      const tracker = new PerformanceTracker(logger, operation);

      try {
        const result = await method.apply(this, args);
        tracker.finish();
        return result;
      } catch (error) {
        tracker.finishWithError(error);
        throw error;
      }
    };
  };
};
