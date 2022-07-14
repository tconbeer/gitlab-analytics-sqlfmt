{{ config({"schema": "legacy"}) }}

with
    source as (

        select *
        from {{ source("engineering", "part_of_product_merge_requests") }}
        qualify
            row_number() over (
                partition by jsontext['plain_diff_path']
                order by
                    array_size(jsontext['merge_request_diffs']) desc, uploaded_at desc
            )
            = 1

    ),
    renamed as (

        select
            jsontext['added_lines']::number as added_lines,
            -- this occasionally has `+` - ie `374+`
            jsontext['real_size']::varchar as real_size,
            jsontext['removed_lines']::number as removed_lines,
            jsontext['plain_diff_path']::varchar as plain_diff_url_path,
            jsontext['merge_request_diff'] ['created_at']::timestamp
            as merge_request_updated_at,
            jsontext['diff_files']::array as file_diffs,
            jsontext['target_branch_name'] as target_branch_name,
            -- get the number after the last dash
            regexp_replace(
                get(
                    split(plain_diff_url_path, '-'),
                    array_size(split(plain_diff_url_path, '-')) - 1
                ),
                '[^0-9]+',
                ''
            )::number as product_merge_request_iid,
            trim(
                array_to_string(
                    array_slice(split(plain_diff_url_path, '-'), 0, -1), '-'
                ),
                '/'
            )::varchar as product_merge_request_project
        from source

    )
select *
from renamed
