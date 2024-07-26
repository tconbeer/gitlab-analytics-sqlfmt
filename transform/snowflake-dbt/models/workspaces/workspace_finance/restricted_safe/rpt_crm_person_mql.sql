select
    mart_crm_person.*,
    '1. New - First Order' as order_type,
    '1) New - First Order' as order_type_grouped,
    'MQLs & Trials' as sales_qualified_source_name,
    dim_date.fiscal_month_name_fy,
    dim_date.fiscal_quarter_name_fy,
    dim_date.fiscal_year,
    dim_date.fiscal_year as date_range_year,
    dim_date.fiscal_quarter_name_fy as date_range_quarter,
    date_trunc(month, dim_date.date_actual) as date_range_month,
    dim_date.date_id as date_range_id
from {{ ref("mart_crm_person") }}
left join {{ ref("dim_date") }} on mart_crm_person.mql_date_first_pt = dim_date.date_day
where is_mql = 1
