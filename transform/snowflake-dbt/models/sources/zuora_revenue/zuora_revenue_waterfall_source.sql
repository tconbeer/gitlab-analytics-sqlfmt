with
    zuora_revenue_waterfall_summary as (

        select *
        from {{ source("zuora_revenue", "zuora_revenue_waterfall_summary") }}
        qualify
            rank() over (
                partition by as_of_prd_id, schd_id, acctg_type_id
                order by incr_updt_dt desc
            )
            = 1

    ),
    renamed as (

        select

            {{ dbt_utils.surrogate_key(["as_of_prd_id", "schd_id", "acctg_type_id"]) }}
            as primary_key,
            as_of_prd_id::varchar as as_of_period_id,
            schd_id::varchar as revenue_contract_schedule_id,
            line_id::varchar as revenue_contract_line_id,
            root_line_id::varchar as root_line_id,
            prd_id::varchar as period_id,
            post_prd_id::varchar as post_period_id,
            sec_atr_val::varchar as security_attribute_value,
            book_id::varchar as book_id,
            client_id::varchar as client_id,
            acctg_seg::varchar as accounting_segment,
            acctg_type_id::varchar as accounting_type_id,
            netting_entry_flag::varchar as is_netting_entry,
            schd_type_flag::varchar as revenue_contract_schedule_type,
            t_at::float as transactional_amount,
            f_at::float as functional_amount,
            r_at::float as reporting_amount,
            crtd_prd_id::varchar as waterfall_created_peridd_id,
            crtd_dt::datetime as waterfall_created_date,
            crtd_by::varchar as waterfall_created_by,
            updt_dt::datetime as waterfall_updated_date,
            updt_by::varchar as waterfall_updated_by,
            incr_updt_dt::datetime as incremental_update_date

        from zuora_revenue_waterfall_summary

    )

select *
from renamed
