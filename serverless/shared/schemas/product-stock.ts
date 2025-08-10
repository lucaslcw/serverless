export interface ProductStockData {
  id: string;
  productId: string;
  type: StockOperationType;
  quantity: number;
  reason: string;
  orderId?: string;
  createdAt: string;
  createdBy?: string;
}

export enum StockOperationType {
  INCREASE = "INCREASE",
  DECREASE = "DECREASE",
}
