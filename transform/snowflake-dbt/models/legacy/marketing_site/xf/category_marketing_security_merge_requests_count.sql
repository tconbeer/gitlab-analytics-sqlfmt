{{ config({"materialized": "view"}) }}

with
    category_marketing_security_merge_requests_path_count as (

        select * from {{ ref("category_marketing_security_merge_requests_path_count") }}

    ),
    marketing_security_merge_request_count_department as (

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

            -- Security
            max(path_count_security) as mr_count_security

        from
            category_marketing_security_merge_requests_path_count
            {{ dbt_utils.group_by(n=6) }}

    )

select *
from marketing_security_merge_request_count_department
