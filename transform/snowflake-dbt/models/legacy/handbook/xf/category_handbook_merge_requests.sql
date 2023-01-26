with
    merge_requests as (select * from {{ ref("gitlab_dotcom_merge_requests_xf") }}),
    mr_files as (

        select
            handbook_file_edited,
            regexp_replace(plain_diff_url_path, '[^0-9]+', '')::number
            as merge_request_iid
        from {{ ref("handbook_merge_requests_files") }}

    ),
    file_classifications as (

        select handbook_path, file_classification
        from {{ ref("handbook_file_classification_mapping") }}

    ),
    joined_to_mr as (

        select
            merge_requests.merge_request_state as merge_request_state,
            merge_requests.updated_at as merge_request_updated_at,
            merge_requests.created_at as merge_request_created_at,
            merge_requests.merge_request_last_edited_at as merge_request_last_edited_at,
            merge_requests.merged_at as merge_request_merged_at,
            mr_files.merge_request_iid as merge_request_iid,
            mr_files.handbook_file_edited as merge_request_path,
            ifnull(
                file_classifications.file_classification, 'unclassified'
            ) as file_classification
        from mr_files
        inner join
            merge_requests
            on mr_files.merge_request_iid = merge_requests.merge_request_iid
            and merge_requests.project_id
            = 7764  -- handbook project
        left join
            file_classifications
            on lower(mr_files.handbook_file_edited)
            like '%' || file_classifications.handbook_path || '%'
        where merge_requests.is_merge_to_master

    ),
    renamed as (

        select
            merge_request_state,
            merge_request_updated_at,
            merge_request_created_at,
            merge_request_last_edited_at,
            merge_request_merged_at,
            merge_request_iid,
            merge_request_path,
            array_agg(distinct file_classification) as merge_request_department_list
        from joined_to_mr {{ dbt_utils.group_by(n=7) }}

    )
select *
from renamed
