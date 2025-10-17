with
    source as (select * from {{ ref("snowflake_grants_to_user_source") }}),
    max_select as (

        select *
        from source
        where snapshot_date = (select max(snapshot_date) from source)

    )

select *
from max_select
