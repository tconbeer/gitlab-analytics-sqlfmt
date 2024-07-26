with
    source as (select * from {{ ref("performance_indicators_yaml_historical") }}),
    max_date as (

        select *
        from source
        where valid_to_date = (select max(valid_to_date) from source)

    )

select *
from max_date
