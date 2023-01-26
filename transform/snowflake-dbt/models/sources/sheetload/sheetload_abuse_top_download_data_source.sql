with
    source as (select * from {{ source("sheetload", "abuse_top_download_data") }}),
    final as (

        select
            nullif(tracked_date, '')::varchar::date as tracked_date,
            try_to_number(legit_users) as legit_users,
            try_to_decimal(legit_gb) as legit_gb,
            try_to_number(blocked_users) as blocked_users,
            try_to_decimal(blocked_gb) as blocked_gb
        from source

    )

select *
from final
