with
    source as (select * from {{ source("sheetload", "kpi_status") }}),
    final as (

        select
            nullif(kpi_grouping, '')::varchar as kpi_grouping,
            nullif(kpi_sub_grouping, '')::varchar as kpi_sub_grouping,
            nullif(kpi, '')::varchar as kpi_name,
            nullif(start_date, '')::varchar::date as start_date,
            nullif(completion_date, '')::varchar::date as completion_date,
            nullif(status, '')::varchar as status,
            nullif(comment, '')::varchar as comment,
            nullif(in_handbook, '')::varchar::boolean as in_handbook,
            nullif(sisense_link, '')::varchar as sisense_link,
            nullif(gitlab_issue, '')::varchar as gitlab_issue,
            nullif(commit_start, '')::varchar as commit_start,
            nullif(commit_handbook_v1, '')::varchar as commit_handbook_v1,
            nullif(is_deleted, 'false')::varchar::boolean as is_deleted,
            nullif(kpi_number, '')::number as kpi_number,
            nullif(version_number, '')::number as version_number,
            nullif(handbook_reference, '')::varchar as handbook_reference,
            nullif(kpi_id, '')::number as kpi_id,
            nullif(kpi_name_pi_yaml, '')::varchar as kpi_name_pi_yaml
        from source

    )

select *
from final
