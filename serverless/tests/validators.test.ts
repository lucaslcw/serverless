import { 
  validateCPF, 
  validateEmail, 
  validateName,
  validateOrderItems, 
  validateCreateOrderData 
} from '../shared/validators';

describe('Validators', () => {
  describe('validateCPF', () => {
    test('should validate valid CPF with dots and dash', () => {
      const result = validateCPF('123.456.789-01');
      expect(result.isValid).toBe(true);
      expect(result.error).toBeUndefined();
    });

    test('should validate valid CPF with only numbers', () => {
      const result = validateCPF('12345678901');
      expect(result.isValid).toBe(true);
      expect(result.error).toBeUndefined();
    });

    test('should reject invalid CPF format', () => {
      const result = validateCPF('123.456.789');
      expect(result.isValid).toBe(false);
      expect(result.error).toContain('Invalid CPF format');
    });

    test('should reject empty CPF', () => {
      const result = validateCPF('');
      expect(result.isValid).toBe(false);
      expect(result.error).toContain('CPF is required');
    });
  });

  describe('validateEmail', () => {
    test('should validate valid email', () => {
      const result = validateEmail('test@example.com');
      expect(result.isValid).toBe(true);
      expect(result.error).toBeUndefined();
    });

    test('should reject invalid email', () => {
      const result = validateEmail('invalid-email');
      expect(result.isValid).toBe(false);
      expect(result.error).toContain('Invalid email format');
    });

    test('should reject empty email', () => {
      const result = validateEmail('');
      expect(result.isValid).toBe(false);
      expect(result.error).toContain('Email is required');
    });
  });

  describe('validateName', () => {
    test('should validate valid name', () => {
      const result = validateName('João Silva');
      expect(result.isValid).toBe(true);
      expect(result.error).toBeUndefined();
    });

    test('should validate name with accents', () => {
      const result = validateName('María José');
      expect(result.isValid).toBe(true);
      expect(result.error).toBeUndefined();
    });

    test('should reject short name', () => {
      const result = validateName('A');
      expect(result.isValid).toBe(false);
      expect(result.error).toContain('at least 2 characters');
    });

    test('should reject empty name', () => {
      const result = validateName('');
      expect(result.isValid).toBe(false);
      expect(result.error).toContain('Name is required');
    });

    test('should reject name with invalid characters', () => {
      const result = validateName('João123');
      expect(result.isValid).toBe(false);
      expect(result.error).toContain('invalid characters');
    });
  });

  describe('validateOrderItems', () => {
    test('should validate valid items array', () => {
      const items = [
        { id: 'item1', quantity: 2 },
        { id: 'item2', quantity: 1 }
      ];
      const result = validateOrderItems(items);
      expect(result.isValid).toBe(true);
    });

    test('should reject empty array', () => {
      const result = validateOrderItems([]);
      expect(result.isValid).toBe(false);
      expect(result.error).toContain('At least one item is required');
    });

    test('should reject non-array input', () => {
      const result = validateOrderItems('not-an-array');
      expect(result.isValid).toBe(false);
      expect(result.error).toContain('Items must be an array');
    });

    test('should reject items with invalid quantity', () => {
      const items = [{ id: 'item1', quantity: 0 }];
      const result = validateOrderItems(items);
      expect(result.isValid).toBe(false);
      expect(result.error).toContain('quantity must be a positive integer');
    });
  });

  describe('validateCreateOrderData', () => {
    test('should validate complete valid order data', () => {
      const data = {
        cpf: '123.456.789-01',
        email: 'test@example.com',
        name: 'João Silva',
        items: [{ id: 'item1', quantity: 2 }]
      };
      const result = validateCreateOrderData(data);
      expect(result.isValid).toBe(true);
    });

    test('should reject data with missing fields', () => {
      const data = {
        cpf: '123.456.789-01'
      };
      const result = validateCreateOrderData(data);
      expect(result.isValid).toBe(false);
      expect(result.error).toContain('Missing required field');
    });
  });
});
