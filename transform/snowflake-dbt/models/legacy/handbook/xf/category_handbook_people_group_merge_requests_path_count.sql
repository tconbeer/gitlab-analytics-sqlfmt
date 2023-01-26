{{ config({"materialized": "view"}) }}

with
    category_handbook_people_group_merge_requests as (

        select * from {{ ref("category_handbook_people_group_merge_requests") }}

    ),
    handbook_people_group_merge_request_path_count_department as (

        select
            -- Foreign Keys 
            merge_request_iid,

            -- Logical Information
            merge_request_path,
            merge_request_state,
            case
                when lower(merge_request_path) like '%/handbook/people-group/%'
                then 1
                else 0
            end as path_count_people_group,

            -- People Group departments 
            iff(
                lower(merge_request_path) like '%/handbook/people-group/engineering/%',
                1,
                0
            ) as path_count_people_group_engineering,

            -- Metadata 
            merge_request_created_at,
            merge_request_last_edited_at,
            merge_request_merged_at,
            merge_request_updated_at

        from category_handbook_people_group_merge_requests

    )

select *
from handbook_people_group_merge_request_path_count_department
