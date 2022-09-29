{{ config(tags=["mnpi_exception"]) }}

with
    hold_source as (

        select * from {{ ref("zuora_revenue_revenue_contract_hold_source") }}

    ),
    final as (

        select distinct

            -- ids
            hold_id as dim_hold_id,

            -- hold details
            hold_type,
            hold_name,
            hold_description,
            hold_level,
            hold_schedule_type

            -- metadata
            hold_created_by,
            hold_created_date,
            hold_update_by,
            hold_update_date

        from hold_source

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
