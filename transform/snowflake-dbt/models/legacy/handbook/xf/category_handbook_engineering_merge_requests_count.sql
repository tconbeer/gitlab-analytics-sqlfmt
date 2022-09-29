{{ config({"materialized": "view"}) }}

with
    category_handbook_engineering_merge_requests_path_count as (

        select *
        from {{ ref("category_handbook_engineering_merge_requests_path_count") }}

    ),
    handbook_engineering_merge_request_count_department as (

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
            max(path_count_engineering) as mr_count_engineering,

            -- Engineering departments
            max(path_count_development) as mr_count_development,
            max(path_count_infrastructure) as mr_count_infrastructure,
            max(path_count_quality) as mr_count_quality,
            max(path_count_security) as mr_count_security,
            max(path_count_support) as mr_count_support,
            max(path_count_ux) as mr_count_ux,
            max(path_count_incubation) as mr_count_incubation

        from
            category_handbook_engineering_merge_requests_path_count
            {{ dbt_utils.group_by(n=6) }}

    )

select *
from handbook_engineering_merge_request_count_department
