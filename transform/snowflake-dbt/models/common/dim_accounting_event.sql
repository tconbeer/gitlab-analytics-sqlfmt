{{ config(tags=["mnpi_exception"]) }}

with
    performance_obligation_source as (

        select *
        from {{ ref("zuora_revenue_revenue_contract_performance_obligation_source") }}

    ),
    final as (

        select distinct

            -- ids
            event_id as dim_accounting_event_id,

            -- event details
            event_name,
            event_type,
            event_column_1,
            event_column_2,
            event_column_3,
            event_column_4,
            event_column_5

            -- metadata
            event_created_by,
            event_created_date,
            event_updated_by,
            event_updated_date

        from performance_obligation_source

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
