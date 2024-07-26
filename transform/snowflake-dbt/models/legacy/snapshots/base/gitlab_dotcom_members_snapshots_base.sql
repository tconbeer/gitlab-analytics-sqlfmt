{{ config({"alias": "gitlab_dotcom_members_snapshots"}) }}

with
    source as (

        select * from {{ source("snapshots", "gitlab_dotcom_members_snapshots") }}

    ),
    renamed as (

        select

            dbt_scd_id::varchar as member_snapshot_id,
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
            "DBT_VALID_FROM"::timestamp as valid_from,
            "DBT_VALID_TO"::timestamp as valid_to

        from source

    )

select *
from renamed
