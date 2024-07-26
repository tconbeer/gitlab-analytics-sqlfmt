with
    source as (select * from {{ source("snowflake", "roles") }}),
    intermediate as (

        select
            name as role_name,
            created_on,
            is_default,
            is_current,
            is_inherited,
            assigned_to_users,
            granted_to_roles,
            granted_roles,
            owner as owner_role,
            comment,
            to_timestamp_ntz(_uploaded_at::number) as snapshot_date
        from source

    )

select *
from intermediate
