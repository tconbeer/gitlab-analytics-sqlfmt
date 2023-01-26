with
    source as (

        select
            added_lines::number as product_merge_request_lines_added,
            real_size::varchar as product_merge_request_files_changed,
            regexp_replace(real_size::varchar, '[^0-9]+', '')::number
            as product_merge_request_files_changed_truncated,
            removed_lines::varchar as product_merge_request_lines_removed,
            product_merge_request_iid,
            product_merge_request_project
        from {{ ref("engineering_part_of_product_merge_requests_source") }}

    ),
    product_projects as (select * from {{ ref("projects_part_of_product") }}),
    project_id_merged_in as (

        select
            product_merge_request_lines_added,
            product_merge_request_files_changed,
            product_merge_request_files_changed_truncated,
            product_merge_request_lines_removed,
            product_merge_request_project,
            product_projects.project_id as product_merge_request_project_id,
            product_merge_request_iid
        from source
        inner join
            product_projects
            on product_projects.project_path = source.product_merge_request_project

    )
select *
from project_id_merged_in
