{{ config(tags=["mnpi_exception"]) }}

with
    sfdc_opportunity_source as (

        select *
        from {{ ref("sfdc_opportunity_source") }}
        where not is_deleted and channel_type is not null

    ),
    final as (

        select distinct
            {{ dbt_utils.surrogate_key(["channel_type"]) }} as dim_channel_type_id,
            channel_type as channel_type_name
        from sfdc_opportunity_source

        UNION ALL

        select
            md5('-1') as dim_channel_type_id,
            'Missing channel_type_name' as channel_type_name

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@jpeguero",
            updated_by="@jpeguero",
            created_date="2021-04-07",
            updated_date="2021-04-28",
        )
    }}
