with
    source as (

        select * from {{ source("driveload", "financial_metrics_program_phase_1") }}

    ),
    renamed as (

        select
            arr_month::date arr_month,
            fiscal_quarter_name_fy::varchar fiscal_quarter_name_fy,
            fiscal_year::number fiscal_year,
            subscription_start_month::timestamp_tz subscription_start_month,
            subscription_end_month::timestamp_tz subscription_end_month,
            zuora_account_id::varchar zuora_account_id,
            zuora_sold_to_country::varchar zuora_sold_to_country,
            zuora_account_name::varchar zuora_account_name,
            zuora_account_number::varchar zuora_account_number,
            dim_crm_account_id::varchar dim_crm_account_id,
            dim_parent_crm_account_id::varchar dim_parent_crm_account_id,
            parent_crm_account_name::varchar parent_crm_account_name,
            parent_crm_account_billing_country
            ::varchar parent_crm_account_billing_country,
            parent_crm_account_sales_segment::varchar parent_crm_account_sales_segment,
            parent_crm_account_industry::varchar parent_crm_account_industry,
            parent_crm_account_owner_team::varchar parent_crm_account_owner_team,
            parent_crm_account_sales_territory
            ::varchar parent_crm_account_sales_territory,
            subscription_name::varchar subscription_name,
            subscription_status::varchar subscription_status,
            subscription_sales_type::varchar subscription_sales_type,
            product_category::varchar product_category,
            delivery::varchar delivery,
            service_type::varchar service_type,
            unit_of_measure::array unit_of_measure,
            mrr::float mrr,
            arr::float arr,
            quantity::float quantity,
            parent_account_cohort_month::date parent_account_cohort_month,
            months_since_parent_account_cohort_start
            ::number months_since_parent_account_cohort_start,
            parent_crm_account_employee_count_band
            ::varchar parent_crm_account_employee_count_band,
            arr_band_calc::varchar arr_band_calc,
            product_name::varchar product_name
        from source

    )

select *
from renamed
