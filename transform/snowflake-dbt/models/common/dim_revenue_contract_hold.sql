{{ config(tags=["mnpi_exception"]) }}

with
    hold_source as (

        select * from {{ ref("zuora_revenue_revenue_contract_hold_source") }}

    ),
    final as (

        select

            -- ids
            revenue_contract_hold_id as dim_revenue_contract_hold_id,

            -- revenue contract hold details
            revenue_contract_hold_comment,
            line_hold_level,
            line_hold_type,
            expiry_field_name,
            expiry_number_type,
            expiry_number,
            revenue_contract_hold_release_comment,
            revenue_contract_hold_release_reason,
            revenue_contract_hold_release_by_name,
            revenue_contract_hold_applied_by_name,

            -- dates
            revenue_contract_hold_release_date,
            revenue_contract_hold_applied_date,
            revenue_contract_hold_expiration_date,
            revenue_hold_start_date,
            revenue_hold_end_date,


            -- metadata
            revenue_contract_hold_created_by,
            revenue_contract_hold_created_date,
            revenue_contract_hold_updated_by,
            revenue_contract_hold_updated_date

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
