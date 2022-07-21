with
    source as (select * from {{ source("snowflake", "users") }}),
    intermediate as (

        select
            name as user_name,
            created_on,
            login_name,
            display_name,
            first_name,
            last_name,
            email,
            comment,
            disabled as is_disabled,
            default_warehouse,
            default_namespace,
            default_role,
            owner as owner_role,
            last_success_login,
            expires_at_time,
            locked_until_time,
            to_timestamp_ntz(_uploaded_at::number) as snapshot_date
        from source

    )

select *
from intermediate
