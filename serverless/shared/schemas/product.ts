export interface ProductData {
  id: string;
  name: string;
  price: number;
  description: string;
  isActive: boolean;
  hasStockControl?: boolean;
}