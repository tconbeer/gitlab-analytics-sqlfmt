{{ config({"materialized": "ephemeral"}) }}

with
    -- explodes the files in the list of diffs
    base as (select * from {{ ref("handbook_merge_requests_source") }}),
    exploded_file_paths as (

        select
            file_diffs.value:file_path::varchar as handbook_file_edited,
            base.plain_diff_url_path as plain_diff_url_path,
            base.merge_request_version_diffs as merge_request_version_diffs,
            base.source_branch_name as source_branch_name
        from base
        inner join table(flatten(input => file_diffs, outer => true)) as file_diffs
        where
            lower(file_diffs.value:file_path) like '%/handbook/%'
            or lower(file_diffs.value:file_path) like '%data/performance_indicators%'

    )
select *
from exploded_file_paths
