with source as (select * from {{ ref("dbt_source_test_results_source") }})

select *
from source
