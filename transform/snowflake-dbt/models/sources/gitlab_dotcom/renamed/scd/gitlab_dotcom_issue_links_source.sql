{{ config({"materialized": "table"}) }}

with
    {{ distinct_source(source=source("gitlab_dotcom", "issue_links")) }},
    renamed as (

        select
            id::number as issue_link_id,
            source_id::number as source_id,
            target_id::number as target_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            valid_from  -- Column was added in distinct_source CTE

        from distinct_source

    )

    {{ scd_type_2(primary_key_renamed="issue_link_id", primary_key_raw="id") }}
