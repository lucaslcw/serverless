import { LeadData } from "./lead";

export enum OrderStatus {
  PENDING = "PENDING",
  PROCESSED = "PROCESSED",
  CANCELLED = "CANCELLED"
}

export interface OrderItem {
  id: string;
  quantity: number;
  unitPrice: number;
  totalPrice: number;
  productName: string;
  hasStockControl: boolean;
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

export type CustomerData = Pick<LeadData, "cpf" | "email" | "name">;

export type OrderData = CustomerData & {
  id: string;
  leadId: string;
  createdAt: string;
  updatedAt: string;
  items: OrderItem[];
  status: OrderStatus;
  totalItems: number;
  totalValue: number;
  addressData: AddressData;
};
