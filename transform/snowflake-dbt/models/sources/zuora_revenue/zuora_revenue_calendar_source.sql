with
    zuora_revenue_calendar as (

        select *
        from {{ source("zuora_revenue", "zuora_revenue_calendar") }}
        qualify rank() over (partition by id order by incr_updt_dt desc) = 1

    ),
    renamed as (

        select

            concat(id::varchar, '01') as period_id,
            period_name::varchar as period_name,
            period_num::varchar as period_number,
            start_date::datetime as calendar_start_date,
            end_date::datetime as calendar_end_date,
            year_start_dt::datetime as year_start_date,
            qtr_num::varchar as quarter_number,
            qtr_start_dt::datetime as quarter_start_date,
            period_year::varchar as period_year,
            qtr_end_dt::datetime as quarter_end_date,
            year_end_dt::datetime as year_end_date,
            client_id::varchar as client_id,
            crtd_by::varchar as calendar_created_by,
            crtd_dt::datetime as calendar_created_date,
            updt_by::varchar as calendar_updated_by,
            updt_dt::datetime as calendar_updated_date,
            incr_updt_dt::datetime as incremental_update_date

        from zuora_revenue_calendar

    )

select *
from renamed
