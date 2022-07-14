with
    base as (

        select *
        from {{ ref("dbt_test_results_source") }}
        qualify
            row_number() over (partition by test_unique_id order by generated_at desc)
            = 1

    ),
    failures as (select * from base where status != 'pass')

select *
from failures
