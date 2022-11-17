with
    monthly_usage_data_all_time as (

        select * from {{ ref("monthly_usage_data_all_time") }}

    ),
    monthly_usage_data_28_days as (

        select * from {{ ref("monthly_usage_data_28_days") }}

    )

select *
from monthly_usage_data_all_time

union

select *
from monthly_usage_data_28_days
