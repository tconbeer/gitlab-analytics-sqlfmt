with
    passing_tests as (

        select *
        from {{ ref("dbt_test_results_source") }}
        where status = 'pass'
        qualify
            row_number() over (
                partition by test_unique_id order by generated_at desc
            ) = 1

    ),
    failing_tests as (select test_unique_id from {{ ref("dbt_failing_tests") }}),
    last_successful_run as (

        select *
        from passing_tests
        where test_unique_id in (select test_unique_id from failing_tests)

    )

select *
from last_successful_run
