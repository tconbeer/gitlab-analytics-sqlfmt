
with
    source as (

        select *
        from {{ ref("gitlab_dotcom_issues_dedupe_source") }}
        where
            created_at::varchar not in (
                '0001-01-01 12:00:00', '1000-01-01 12:00:00', '10000-01-01 12:00:00'
            )
            and left(created_at::varchar, 10) != '1970-01-01'

    ),
    renamed as (

        select

            id::number as issue_id,
            iid::number as issue_iid,
            author_id::number as author_id,
            source.project_id::number as project_id,
            milestone_id::number as milestone_id,
            sprint_id::number as sprint_id,
            updated_by_id::number as updated_by_id,
            last_edited_by_id::number as last_edited_by_id,
            moved_to_id::number as moved_to_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            last_edited_at::timestamp as issue_last_edited_at,
            closed_at::timestamp as issue_closed_at,
            confidential::boolean as is_confidential,
            title::varchar as issue_title,
            description::varchar as issue_description,

            -- Override state by mapping state_id. See issue #3344.
            {{ map_state_id("state_id") }} as state,

            weight::number as weight,
            due_date::date as due_date,
            lock_version::number as lock_version,
            time_estimate::number as time_estimate,
            discussion_locked::boolean as has_discussion_locked,
            closed_by_id::number as closed_by_id,
            relative_position::number as relative_position,
            service_desk_reply_to::varchar as service_desk_reply_to,
            state_id::number as state_id,
            duplicated_to_id::number as duplicated_to_id,
            promoted_to_epic_id::number as promoted_to_epic_id,
            issue_type::number as issue_type

        from source

    )

select *
from renamed
