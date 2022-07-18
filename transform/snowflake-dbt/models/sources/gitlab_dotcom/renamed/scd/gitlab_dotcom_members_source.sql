{{ config({"materialized": "table"}) }}

with
    {{ distinct_source(source=source("gitlab_dotcom", "members")) }},
    renamed as (

        select

            id::number as member_id,
            access_level::number as access_level,
            source_id::number as source_id,
            source_type as member_source_type,
            user_id::number as user_id,
            notification_level::number as notification_level,
            type as member_type,
            created_at::timestamp as invite_created_at,
            created_by_id::number as created_by_id,
            invite_accepted_at::timestamp as invite_accepted_at,
            requested_at::timestamp as requested_at,
            expires_at::timestamp as expires_at,
            ldap::boolean as has_ldap,
            override::boolean as has_override,
            valid_from  -- Column was added in distinct_source CTE

        from distinct_source

    )

    {{ scd_type_2(primary_key_renamed="member_id", primary_key_raw="id") }}
