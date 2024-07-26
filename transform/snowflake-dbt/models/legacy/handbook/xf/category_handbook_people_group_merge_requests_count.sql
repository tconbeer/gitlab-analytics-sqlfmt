{{ config({"materialized": "view"}) }}

with
    category_handbook_people_group_merge_requests_path_count as (

        select *
        from {{ ref("category_handbook_people_group_merge_requests_path_count") }}

    ),
    handbook_people_group_merge_request_count_department as (

        select
            -- Foreign Keys
            merge_request_iid,

            -- Metadata
            merge_request_created_at,
            merge_request_last_edited_at,
            merge_request_merged_at,
            merge_request_updated_at,

            -- Logical Information
            merge_request_state,
            max(path_count_people_group) as mr_count_people_group,

            -- People Group departments
            max(
                path_count_people_group_engineering
            ) as mr_count_people_group_engineering

        from
            category_handbook_people_group_merge_requests_path_count
            {{ dbt_utils.group_by(n=6) }}

    )

select *
from handbook_people_group_merge_request_count_department
