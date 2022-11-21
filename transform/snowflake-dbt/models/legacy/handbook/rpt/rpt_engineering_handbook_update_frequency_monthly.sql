{{ config({"materialized": "view"}) }}

with
    category_handbook_engineering_merge_requests_count as (

        select * from {{ ref("category_handbook_engineering_merge_requests_count") }}

    ),
    handbook_engineering_total_count_department as (

        select
            date_trunc('MONTH', merge_request_merged_at) as month_merged_at,
            sum(mr_count_engineering) as mr_count_engineering,
            sum(mr_count_ux) as mr_count_ux,
            sum(mr_count_security) as mr_count_security,
            sum(mr_count_infrastructure) as mr_count_infrastructure,
            sum(mr_count_development) as mr_count_development,
            sum(mr_count_quality) as mr_count_quality,
            sum(mr_count_support) as mr_count_support,
            sum(mr_count_incubation) as mr_count_incubation
        from category_handbook_engineering_merge_requests_count
        where merge_request_state = 'merged' and merge_request_merged_at is not null
        group by 1

    )

select *
from handbook_engineering_total_count_department
