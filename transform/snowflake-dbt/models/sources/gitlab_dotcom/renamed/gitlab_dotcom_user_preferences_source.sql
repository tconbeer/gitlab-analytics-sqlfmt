with
    source as (select * from {{ ref("gitlab_dotcom_user_preferences_dedupe_source") }}),
    renamed as (

        select
            user_id::number as user_id,
            issue_notes_filter::varchar as issue_notes_filter,
            merge_request_notes_filter::varchar as merge_request_notes_filter,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            epics_sort::varchar as epic_sort,
            roadmap_epics_state::varchar as roadmap_epics_state,
            epic_notes_filter::varchar as epic_notes_filter,
            issues_sort::varchar as issues_sort,
            merge_requests_sort::varchar as merge_requests_sort,
            roadmaps_sort::varchar as roadmaps_sort,
            first_day_of_week::varchar as first_day_of_week,
            timezone::varchar as timezone,
            time_display_relative::boolean as time_display_relative,
            time_format_in_24h::boolean as time_format_in_24h,
            projects_sort::varchar as projects_sort,
            show_whitespace_in_diffs::boolean as show_whitespace_in_diffs,
            sourcegraph_enabled::boolean as sourcegraph_enabled,
            setup_for_company::boolean as setup_for_company,
            render_whitespace_in_code::boolean as render_whitespace_in_code,
            tab_width::varchar as tab_width,
            experience_level::number as experience_level,
            view_diffs_file_by_file::boolean as view_diffs_file_by_file

        from source

    )

select *
from renamed
