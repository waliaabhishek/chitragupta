export interface ChargebackFilters {
  start_date: string | null; // YYYY-MM-DD
  end_date: string | null;
  identity_id: string | null;
  product_type: string | null;
  resource_id: string | null;
  cost_type: string | null;
  timezone: string | null;
  tag_key: string | null;
  tag_value: string | null;
}

export interface TopicAttributionFilters {
  start_date: string | null; // YYYY-MM-DD
  end_date: string | null;
  cluster_resource_id: string | null;
  topic_name: string | null;
  product_type: string | null;
  attribution_method: string | null;
  timezone: string | null;
}

export interface BillingFilters {
  start_date: string | null; // YYYY-MM-DD
  end_date: string | null;
  product_type: string | null;
  resource_id: string | null;
  timezone: string | null;
}

export interface IdentityFilters {
  search: string | null;
  identity_type: string | null;
  tag_key: string | null;
  tag_value: string | null;
}

export interface ResourceFilters {
  search: string | null;
  resource_type: string | null;
  status: string | null;
  tag_key: string | null;
  tag_value: string | null;
}

export interface TagFilters {
  tag_key: string | null;
  entity_type: string | null;
}
