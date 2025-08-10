import { AddressData, CustomerData, PaymentData } from "./order";

export enum PaymentStatus {
  PENDING = "PENDING",
  APPROVED = "APPROVED",
  DECLINED = "DECLINED",
  ERROR = "ERROR"
}

export interface TransactionData {
  id: string;
  amount: number;
  orderId: string;
  authCode?: string;
  processingTime?: number;
  paymentData: PaymentData;
  addressData: AddressData;
  customerData: CustomerData;
  paymentStatus: PaymentStatus;
  createdAt: string;
  updatedAt: string;
}