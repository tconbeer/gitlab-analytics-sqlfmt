{{ config(tags=["mnpi"]) }}

with
    source as (

        select
            accounting_period::date as accounting_period,
            account_id::varchar as account_id,
            crm_account_id::varchar as crm_account_id,
            rate_plan_charge_id::varchar as rate_plan_charge_id,
            dim_subscription_id::varchar as dim_subscription_id,
            subscription_name::varchar as subscription_name,
            subscription_status::varchar as subscription_status,
            dim_product_detail_id::varchar as dim_product_detail_id,
            mrr::number as mrr,
            delta_tcv::number as delta_tcv,
            unit_of_measure::varchar as unit_of_measure,
            quantity::number as quantity,
            effective_start_date::date as effective_start_date,
            effective_end_date::date as effective_end_date,
            created_by::varchar as created_by,
            created_date::date as created_date,
            updated_by::varchar as updated_by,
            updated_date::date as updated_date
        from {{ source("sheetload", "manual_arr_true_up_allocation") }}

    )

select *
from source
