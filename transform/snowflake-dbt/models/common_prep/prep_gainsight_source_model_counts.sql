WITH counts AS (
  
   SELECT 
    'version_raw_usage_data_source' AS model_name,
    COUNT(*) AS row_count 
   FROM {{ ref('version_raw_usage_data_source') }}

   UNION ALL

   SELECT 
    'version_usage_data_source' AS model_name,
    COUNT(*) AS row_count
   FROM  {{ ref('version_usage_data_source') }}
    
   UNION ALL

   SELECT 
    'zuora_subscription_source' AS model_name,
    COUNT(*) AS row_count
   FROM  {{ ref('zuora_subscription_source') }}
       
   UNION ALL

   SELECT 
    'zuora_rate_plan_source' AS model_name,
    COUNT(*) AS row_count
   FROM  {{ ref('zuora_rate_plan_source') }}
       
   UNION ALL

   SELECT 
    'customers_db_orders_source' AS model_name,
    COUNT(*) AS row_count
   FROM  {{ ref('customers_db_orders_source') }}
       
   UNION ALL

   SELECT 
    'customers_db_license_seat_links_source' AS model_name,
    COUNT(*) AS row_count
   FROM  {{ ref('customers_db_license_seat_links_source') }}

)


{{ dbt_audit(
    cte_ref="counts",
    created_by="@snalamaru",
    updated_by="@ischweickartDD",
    created_date="2021-02-19",
    updated_date="2021-04-05"
) }}