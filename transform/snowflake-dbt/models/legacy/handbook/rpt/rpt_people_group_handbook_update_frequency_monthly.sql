{{ config({"materialized": "view"}) }}

with
    category_handbook_people_group_merge_requests_count as (

        select * from {{ ref("category_handbook_people_group_merge_requests_count") }}

    ),
    handbook_people_group_total_count_department as (

        select
            date_trunc('MONTH', merge_request_merged_at) as month_merged_at,
            sum(mr_count_people_group) as mr_count_people_group,
            sum(mr_count_people_group_engineering) as mr_count_people_group_engineering
        from category_handbook_people_group_merge_requests_count
        where merge_request_state = 'merged' and merge_request_merged_at is not null
        group by 1

    )

select *
from handbook_people_group_total_count_department
