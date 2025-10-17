{{ config(tags=["mnpi_exception"]) }}

with
    channel_type as (

        select dim_channel_type_id, channel_type_name
        from {{ ref("prep_channel_type") }}
    )

    {{
        dbt_audit(
            cte_ref="channel_type",
            created_by="@jpeguero",
            updated_by="@jpeguero",
            created_date="2021-04-28",
            updated_date="2021-04-28",
        )
    }}
