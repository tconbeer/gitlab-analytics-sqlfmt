with
    source as (select * from {{ ref("flaky_tests_historical") }}),
    max_date as (

        select *
        from source
        where snapshot_date = (select max(snapshot_date) from source)

    )

select *
from max_date
