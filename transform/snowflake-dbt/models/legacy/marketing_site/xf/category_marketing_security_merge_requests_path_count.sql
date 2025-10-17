{{ config({"materialized": "view"}) }}

with
    category_marketing_security_merge_requests as (

        select * from {{ ref("category_marketing_security_merge_requests") }}

    ),
    marketing_security_merge_request_path_count_department as (

        select
            -- Foreign Keys 
            merge_request_iid,

            -- Logical Information
            merge_request_path,
            merge_request_state,

            -- Security
            iff(
                lower(merge_request_path) like '%sites/marketing/source/security/%',
                1,
                0
            ) as path_count_security,

            -- Metadata 
            merge_request_created_at,
            merge_request_last_edited_at,
            merge_request_merged_at,
            merge_request_updated_at

        from category_marketing_security_merge_requests

    )

select *
from marketing_security_merge_request_path_count_department
