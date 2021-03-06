{{ config(tags=["mnpi_exception"]) }}

with
    dr_partner_engagement as (

        select dim_dr_partner_engagement_id, dr_partner_engagement_name
        from {{ ref("prep_dr_partner_engagement") }}
    )

    {{
        dbt_audit(
            cte_ref="dr_partner_engagement",
            created_by="@iweeks",
            updated_by="@iweeks",
            created_date="2021-04-07",
            updated_date="2021-04-07",
        )
    }}
