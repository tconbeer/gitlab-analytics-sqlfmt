SELECT
  mart_crm_opportunity.*,
  dim_date.fiscal_year                     AS date_range_year,
  dim_date.fiscal_quarter_name_fy          AS date_range_quarter,
  DATE_TRUNC(month, dim_date.date_actual)  AS date_range_month,
  dim_date.date_id                         AS date_range_id,
  dim_date.fiscal_month_name_fy,
  dim_date.fiscal_quarter_name_fy,  
  dim_date.fiscal_year,
  dim_date.first_day_of_fiscal_quarter
FROM {{ ref('mart_crm_opportunity') }}
LEFT JOIN {{ ref('dim_date') }} 
  ON mart_crm_opportunity.close_date = dim_date.date_actual
