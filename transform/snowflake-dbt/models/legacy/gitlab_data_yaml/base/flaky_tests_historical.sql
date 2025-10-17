with
    source as (select * from {{ ref("flaky_tests_source") }}),
    filtered as (select * from source where rank = 1)

select *
from filtered
