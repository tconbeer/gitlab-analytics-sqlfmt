{{ config(tags=["mnpi_exception"]) }}

with
    schedule_source as (

        select * from {{ ref("zuora_revenue_revenue_contract_schedule_source") }}

    ),
    final as (

        select distinct

            -- ids
            revenue_contract_schedule_id as dim_revenue_contract_schedule_id,

            -- dates
            exchange_rate_date,
            post_date,

            -- currency
            transactional_currency,
            schedule_type,

            -- metadata
            revenue_contract_schedule_created_by,
            revenue_contract_schedule_created_date,
            revenue_contract_schedule_updated_by,
            revenue_contract_schedule_updated_date

        from schedule_source

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@michellecooper",
            updated_by="@michellecooper",
            created_date="2021-06-21",
            updated_date="2021-06-21",
        )
    }}
