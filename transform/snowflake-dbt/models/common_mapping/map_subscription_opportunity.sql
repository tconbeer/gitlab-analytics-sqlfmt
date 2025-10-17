{{ config(tags=["mnpi_exception"]) }}

with
    prep_subscription_opportunity_mapping as (

        select * from {{ ref("prep_subscription_opportunity_mapping") }}

    ),
    final_mapping as (

        select distinct
            dim_subscription_id,
            dim_crm_opportunity_id,
            is_questionable_opportunity_mapping
        from prep_subscription_opportunity_mapping
        where dim_crm_opportunity_id is not null

    )

    {{
        dbt_audit(
            cte_ref="final_mapping",
            created_by="@michellecooper",
            updated_by="@michellecooper",
            created_date="2021-11-10",
            updated_date="2021-11-16",
        )
    }}
