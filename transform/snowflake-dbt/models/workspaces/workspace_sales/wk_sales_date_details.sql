{{ config(alias="date_details") }}

with
    date_details as (

        select
            *,
            -- beggining of the week
            is_first_day_of_fiscal_quarter_week
            as is_first_day_of_fiscal_quarter_week_flag,
            fiscal_quarter_number_absolute as quarter_number

        from {{ ref("date_details") }}
        order by 1 desc

    )

select *
from date_details
