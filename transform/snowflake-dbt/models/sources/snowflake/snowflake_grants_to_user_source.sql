with
    source as (select * from {{ source("snowflake", "grants_to_user") }}),
    intermediate as (

        select
            role as role_name,
            granted_to as granted_to_type,
            grantee_name,
            to_timestamp_ntz(_uploaded_at::number) as snapshot_date,
            created_on
        from source

    )

select *
from intermediate
