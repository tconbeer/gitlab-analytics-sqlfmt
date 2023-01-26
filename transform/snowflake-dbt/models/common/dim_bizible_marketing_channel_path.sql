with
    bizible_marketing_channel_path as (

        select
            dim_bizible_marketing_channel_path_id, bizible_marketing_channel_path_name
        from {{ ref("prep_bizible_marketing_channel_path") }}
    )

    {{
        dbt_audit(
            cte_ref="bizible_marketing_channel_path",
            created_by="@paul_armstrong",
            updated_by="@mcooperDD",
            created_date="2020-11-13",
            updated_date="2021-02-26",
        )
    }}
