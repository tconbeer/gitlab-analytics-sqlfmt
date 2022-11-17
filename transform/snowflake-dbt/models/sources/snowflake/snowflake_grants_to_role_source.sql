with
    source as (select * from {{ source("snowflake", "grants_to_role") }}),
    intermediate as (

        select
            name as role_name,
            created_on,
            privilege,
            granted_on,
            granted_to as granted_to_type,
            grantee_name as grantee_user_name,
            grant_option,
            granted_by,
            to_timestamp_ntz(_uploaded_at::number) as snapshot_date
        from source

    )

select *
from intermediate
