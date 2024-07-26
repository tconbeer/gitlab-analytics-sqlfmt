{{ config({"materialized": "view"}) }}

with
    category_marketing_security_merge_requests_count as (

        select * from {{ ref("category_marketing_security_merge_requests_count") }}

    ),
    marketing_security_total_count_department as (

        select
            date_trunc('MONTH', merge_request_merged_at) as month_merged_at,
            sum(mr_count_security) as mr_count_security
        from category_marketing_security_merge_requests_count
        where merge_request_state = 'merged' and merge_request_merged_at is not null
        group by 1

    )

select *
from marketing_security_total_count_department
