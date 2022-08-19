{{
    config(
        {
            "materialized": "view",
        }
    )
}}
with
    touchpoints as (select * from {{ ref("sfdc_bizible_touchpoint_source") }}),
    final as (

        select distinct
            bizible_marketing_channel_path as bizible_marketing_channel_path,
            {{ map_marketing_channel_path("bizible_marketing_channel_path") }}
            as bizible_marketing_channel_path_name_grouped
        from touchpoints
        where bizible_touchpoint_position like '%FT%'

    )


    {{
        dbt_audit(
            cte_ref="final",
            created_by="@paul_armstrong",
            updated_by="@mcooperDD",
            created_date="2020-11-13",
            updated_date="2021-02-26",
        )
    }}
