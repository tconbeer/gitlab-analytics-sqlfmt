{{ config({"materialized": "table"}) }}

with
    {{ distinct_source(source=source("gitlab_dotcom", "group_group_links")) }},
    renamed as (

        select

            id::number as group_group_link_id,
            shared_group_id::number as shared_group_id,
            shared_with_group_id::number as shared_with_group_id,
            group_access::number as group_access,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            expires_at::timestamp as expires_at,
            valid_from  -- Column was added in distinct_source CTE

        from distinct_source

    )

    {{ scd_type_2(primary_key_renamed="group_group_link_id", primary_key_raw="id") }}
