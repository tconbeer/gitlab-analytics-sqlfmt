with
    source as (select * from {{ source("rspec", "overall_time") }}),
    renamed as (

        select
            commit_hash::varchar as commit,
            commit_time::timestamp_tz as commit_at_time,
            total_time::float as total_time_taken_seconds,
            number_of_tests::float as number_of_tests,
            time_per_single_test::float as time_per_single_test_seconds,
            total_queries::float as total_queries,
            total_query_time::float as total_query_time_seconds,
            total_requests::float as total_requests,
            _updated_at::float as updated_at
        from source

    )

select *
from renamed
