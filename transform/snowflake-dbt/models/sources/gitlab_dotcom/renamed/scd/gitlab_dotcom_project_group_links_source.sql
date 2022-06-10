{{ config({"materialized": "table"}) }}

with
    {{ distinct_source(source=source("gitlab_dotcom", "project_group_links")) }}

    ,
    renamed as (

        select

            id::number as project_group_link_id,
            project_id::number as project_id,
            group_id::number as group_id,
            group_access::number as group_access,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            expires_at::timestamp as expires_at,
            valid_from  -- Column was added in distinct_source CTE

        from distinct_source

    )

    {{ scd_type_2(primary_key_renamed="project_group_link_id", primary_key_raw="id") }}
