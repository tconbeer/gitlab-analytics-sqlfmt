with
    source as (select * from {{ source("sheetload", "abuse_top_ci_data") }}),
    final as (

        select
            nullif(tracked_date, '')::varchar::date as tracked_date,
            try_to_number(legit_users) as legit_users,
            try_to_number(legit_hours) as legit_hours,
            try_to_number(blocked_users) as blocked_users,
            try_to_number(blocked_hours) as blocked_hours
        from source

    )

select *
from final
