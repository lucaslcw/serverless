export interface ValidationResult {
  isValid: boolean;
  error?: string;
}

export interface OrderItem {
  id: string;
  quantity: number;
}

export interface CreateOrderData {
  cpf: string;
  email: string;
  name: string;
  items: OrderItem[];
}

export const validateCPF = (cpf: string): ValidationResult => {
  if (!cpf || typeof cpf !== "string") {
    return { isValid: false, error: "CPF is required and must be a string" };
  }

  const cpfRegex = /^\d{11}$|^\d{3}\.\d{3}\.\d{3}-\d{2}$/;
  if (!cpfRegex.test(cpf)) {
    return {
      isValid: false,
      error: "Invalid CPF format. Use 11 digits or XXX.XXX.XXX-XX format",
    };
  }

  return { isValid: true };
};

export const validateEmail = (email: string): ValidationResult => {
  if (!email || typeof email !== "string") {
    return { isValid: false, error: "Email is required and must be a string" };
  }

  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!emailRegex.test(email)) {
    return { isValid: false, error: "Invalid email format" };
  }

  return { isValid: true };
};

export const validateName = (name: string): ValidationResult => {
  if (!name || typeof name !== "string") {
    return { isValid: false, error: "Name is required and must be a string" };
  }

  const trimmedName = name.trim();
  if (trimmedName.length < 2) {
    return { isValid: false, error: "Name must have at least 2 characters" };
  }

  if (trimmedName.length > 100) {
    return { isValid: false, error: "Name must not exceed 100 characters" };
  }

  const nameRegex = /^[a-zA-ZÀ-ÿ\s'-]+$/;
  if (!nameRegex.test(trimmedName)) {
    return { isValid: false, error: "Name contains invalid characters" };
  }

  return { isValid: true };
};

export const validateOrderItems = (items: any): ValidationResult => {
  if (!items || !Array.isArray(items)) {
    return { isValid: false, error: "Items must be an array" };
  }

  if (items.length === 0) {
    return { isValid: false, error: "At least one item is required" };
  }

  for (let i = 0; i < items.length; i++) {
    const item = items[i];

    if (!item.id || typeof item.id !== "string") {
      return {
        isValid: false,
        error: `Item at index ${i}: id is required and must be a string`,
      };
    }

    if (
      typeof item.quantity !== "number" ||
      item.quantity <= 0 ||
      !Number.isInteger(item.quantity)
    ) {
      return {
        isValid: false,
        error: `Item at index ${i}: quantity must be a positive integer`,
      };
    }
  }

  return { isValid: true };
};

export const validateCreateOrderData = (data: any): ValidationResult => {
  if (!data || typeof data !== "object") {
    return {
      isValid: false,
      error: "Request body is required and must be an object",
    };
  }

  const requiredFields = ["cpf", "email", "name", "items"];
  for (const field of requiredFields) {
    if (!(field in data)) {
      return { isValid: false, error: `Missing required field: ${field}` };
    }
  }

  const cpfValidation = validateCPF(data.cpf);
  if (!cpfValidation.isValid) {
    return cpfValidation;
  }

  const emailValidation = validateEmail(data.email);
  if (!emailValidation.isValid) {
    return emailValidation;
  }

  const nameValidation = validateName(data.name);
  if (!nameValidation.isValid) {
    return nameValidation;
  }

  const itemsValidation = validateOrderItems(data.items);
  if (!itemsValidation.isValid) {
    return itemsValidation;
  }

  return { isValid: true };
};

export const createErrorResponse = (statusCode: number, error: string) => {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ error }),
  };
};

export const createSuccessResponse = (statusCode: number, data: any) => {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(data),
  };
};

export const sanitizeCreateOrderData = (data: any): CreateOrderData => {
  return {
    cpf: data.cpf.trim(),
    email: data.email.toLowerCase().trim(),
    name: data.name.trim(),
    items: data.items.map((item: any) => ({
      id: item.id.trim(),
      quantity: parseInt(item.quantity, 10),
    })),
  };
};
