select
    mart_crm_opportunity.*,
    dim_date.fiscal_year as date_range_year,
    dim_date.fiscal_quarter_name_fy as date_range_quarter,
    date_trunc(month, dim_date.date_actual) as date_range_month,
    dim_date.date_id as date_range_id,
    dim_date.fiscal_month_name_fy,
    dim_date.fiscal_quarter_name_fy,
    dim_date.fiscal_year,
    dim_date.first_day_of_fiscal_quarter
from {{ ref("mart_crm_opportunity") }}
left join
    {{ ref("dim_date") }} on mart_crm_opportunity.close_date = dim_date.date_actual
