with
    test_results as (select * from {{ ref("dbt_test_results_source") }}),
    tests as (select * from {{ ref("dbt_test_source") }}),
    joined as (

        select
            test_results.test_execution_time_elapsed,
            test_results.test_unique_id,
            test_results.status as test_status,
            test_results.message as test_message,
            test_results.generated_at as results_generated_at,
            tests.name as test_name,
            tests.alias as test_alias,
            tests.test_type,
            tests.package_name,
            tests.tags as test_tags,
            tests.severity as test_severity,
            tests.referrences as test_references
        from test_results
        inner join tests on test_results.test_unique_id = tests.unique_id

    )

select *
from joined
