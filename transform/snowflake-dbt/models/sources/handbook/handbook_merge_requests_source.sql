with
    filtered_attributes as (

        select
            jsontext['diff_files']::array as file_diffs,
            jsontext['branch_name']::varchar as source_branch_name,
            jsontext['merge_request_diffs']::array as merge_request_version_diffs,
            jsontext['plain_diff_path']::varchar as plain_diff_url_path
        from {{ source("handbook", "handbook_merge_requests") }}
        qualify
            row_number() over (
                partition by jsontext['plain_diff_path']
                order by
                    array_size(jsontext['merge_request_diffs']) desc, uploaded_at desc
            ) = 1


    )

select *
from filtered_attributes
