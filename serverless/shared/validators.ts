import { BodyData } from "../lambdas/order/initialize-order";
import { CustomerData } from "./schemas/order";

export interface ValidationResult {
  isValid: boolean;
  error?: string;
}

export interface OrderItem {
  id: string;
  quantity: number;
}

export interface PaymentData {
  cardNumber: string;
  cardHolderName: string;
  expiryMonth: string;
  expiryYear: string;
  cvv: string;
}

export interface AddressData {
  street: string;
  number: string;
  complement?: string;
  neighborhood: string;
  city: string;
  state: string;
  zipCode: string;
  country: string;
}

export interface SanitizedInitializedOrderData {
  items: OrderItem[];
  customerData: CustomerData;
  paymentData: PaymentData;
  addressData: AddressData;
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

export const validatePaymentData = (paymentData: any): ValidationResult => {
  if (!paymentData || typeof paymentData !== "object") {
    return {
      isValid: false,
      error: "Payment data is required and must be an object",
    };
  }

  const requiredFields = [
    "cardNumber",
    "cardHolderName",
    "expiryMonth",
    "expiryYear",
    "cvv",
  ];
  for (const field of requiredFields) {
    if (!(field in paymentData)) {
      return {
        isValid: false,
        error: `Missing required payment field: ${field}`,
      };
    }
  }

  if (
    typeof paymentData.cardNumber !== "string" ||
    !/^\d{16}$/.test(paymentData.cardNumber)
  ) {
    return { isValid: false, error: "Card number must be 16 digits" };
  }

  if (
    typeof paymentData.cardHolderName !== "string" ||
    paymentData.cardHolderName.trim().length < 2
  ) {
    return {
      isValid: false,
      error: "Card holder name is required and must have at least 2 characters",
    };
  }

  const expiryMonth = parseInt(paymentData.expiryMonth, 10);
  if (isNaN(expiryMonth) || expiryMonth < 1 || expiryMonth > 12) {
    return { isValid: false, error: "Expiry month must be between 1 and 12" };
  }

  const expiryYear = parseInt(paymentData.expiryYear, 10);
  const currentYear = new Date().getFullYear();
  if (
    isNaN(expiryYear) ||
    expiryYear < currentYear ||
    expiryYear > currentYear + 10
  ) {
    return {
      isValid: false,
      error:
        "Expiry year must be valid and not more than 10 years in the future",
    };
  }

  if (
    typeof paymentData.cvv !== "string" ||
    !/^\d{3,4}$/.test(paymentData.cvv)
  ) {
    return { isValid: false, error: "CVV must be 3 or 4 digits" };
  }

  return { isValid: true };
};

export const validateAddressData = (addressData: any): ValidationResult => {
  if (!addressData || typeof addressData !== "object") {
    return {
      isValid: false,
      error: "Address data is required and must be an object",
    };
  }

  const requiredFields = [
    "street",
    "number",
    "neighborhood",
    "city",
    "state",
    "zipCode",
    "country",
  ];
  for (const field of requiredFields) {
    if (!(field in addressData)) {
      return {
        isValid: false,
        error: `Missing required address field: ${field}`,
      };
    }
  }

  const stringFields = [
    "street",
    "number",
    "neighborhood",
    "city",
    "state",
    "country",
  ];
  for (const field of stringFields) {
    if (
      typeof addressData[field] !== "string" ||
      addressData[field].trim().length === 0
    ) {
      return {
        isValid: false,
        error: `${field} is required and must be a non-empty string`,
      };
    }
  }

  if (
    typeof addressData.zipCode !== "string" ||
    !/^\d{5}-?\d{3}$/.test(addressData.zipCode)
  ) {
    return {
      isValid: false,
      error: "ZIP code must be in format XXXXX-XXX or XXXXXXXX",
    };
  }

  if (addressData.complement && typeof addressData.complement !== "string") {
    return { isValid: false, error: "Complement must be a string if provided" };
  }

  return { isValid: true };
};

export const validateInitializeOrderData = (
  data: BodyData
): ValidationResult => {
  if (!data || typeof data !== "object") {
    return {
      isValid: false,
      error: "Request body is required and must be an object",
    };
  }

  const requiredFields = [
    "customerData",
    "paymentData",
    "addressData",
  ];
  for (const field of requiredFields) {
    if (!(field in data)) {
      return { isValid: false, error: `Missing required field: ${field}` };
    }
  }

  const cpfValidation = validateCPF(data.customerData.cpf);
  if (!cpfValidation.isValid) {
    return cpfValidation;
  }

  const emailValidation = validateEmail(data.customerData.email);
  if (!emailValidation.isValid) {
    return emailValidation;
  }

  const nameValidation = validateName(data.customerData.name);
  if (!nameValidation.isValid) {
    return nameValidation;
  }

  const itemsValidation = validateOrderItems(data.items);
  if (!itemsValidation.isValid) {
    return itemsValidation;
  }

  const paymentValidation = validatePaymentData(data.paymentData);
  if (!paymentValidation.isValid) {
    return paymentValidation;
  }

  const addressValidation = validateAddressData(data.addressData);
  if (!addressValidation.isValid) {
    return addressValidation;
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

export const sanitizeInitializeOrderData = (data: BodyData): SanitizedInitializedOrderData => {
  return {
    items: data.items.map((item: any) => ({
      id: item.id.trim(),
      quantity: parseInt(item.quantity, 10),
    })),
    customerData: {
      cpf: data.customerData.cpf.trim(),
      email: data.customerData.email.toLowerCase().trim(),
      name: data.customerData.name.trim(),
    },
    paymentData: {
      cardNumber: data.paymentData.cardNumber.replace(/\s/g, ""),
      cardHolderName: data.paymentData.cardHolderName.trim().toUpperCase(),
      expiryMonth: data.paymentData.expiryMonth.toString().padStart(2, "0"),
      expiryYear: data.paymentData.expiryYear.toString(),
      cvv: data.paymentData.cvv.trim(),
    },
    addressData: {
      street: data.addressData.street.trim(),
      number: data.addressData.number.trim(),
      complement: data.addressData.complement
        ? data.addressData.complement.trim()
        : undefined,
      neighborhood: data.addressData.neighborhood.trim(),
      city: data.addressData.city.trim(),
      state: data.addressData.state.trim().toUpperCase(),
      zipCode: data.addressData.zipCode
        .replace(/\D/g, "")
        .replace(/(\d{5})(\d{3})/, "$1-$2"),
      country: data.addressData.country.trim().toUpperCase(),
    },
  };
};
