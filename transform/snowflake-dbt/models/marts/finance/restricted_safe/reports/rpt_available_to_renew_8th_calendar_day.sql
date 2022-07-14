with
    snapshot_dates as (
        -- Use the 8th calendar day to snapshot ATR
        select distinct first_day_of_month, snapshot_date_fpa
        from {{ ref("dim_date") }}
        order by 1 desc

    ),
    mart_available_to_renew_snapshot as (

        select * from {{ ref("mart_available_to_renew_snapshot_model") }}

    ),
    final as (

        select *
        from mart_available_to_renew_snapshot
        inner join
            snapshot_dates
            on mart_available_to_renew_snapshot.snapshot_date
            = snapshot_dates.snapshot_date_fpa

    )

select *
from final
