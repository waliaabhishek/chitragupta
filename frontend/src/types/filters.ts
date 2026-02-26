export interface ChargebackFilters {
  start_date: string | null; // YYYY-MM-DD
  end_date: string | null;
  identity_id: string | null;
  product_type: string | null;
  resource_id: string | null;
  cost_type: string | null;
}
