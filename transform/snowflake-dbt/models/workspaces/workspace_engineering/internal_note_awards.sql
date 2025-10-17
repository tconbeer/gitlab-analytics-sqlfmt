with
    internal_projects as (

        select
            ultimate_parent_namespace_id,
            dim_namespace_id as namespace_id,
            dim_project_id as project_id
        from {{ ref("dim_project") }}
        where namespace_is_internal = true

    ),
    merge_requests as (

        select
            dim_merge_request.dim_merge_request_id as merge_request_id,
            dim_merge_request.dim_project_id as project_id,
            internal_projects.ultimate_parent_namespace_id,
            internal_projects.namespace_id
        from {{ ref("dim_merge_request") }}
        inner join
            internal_projects
            on internal_projects.project_id = dim_merge_request.dim_project_id

    ),
    issues as (

        select
            dim_issue.dim_issue_id as issue_id,
            dim_issue.dim_project_id as project_id,
            internal_projects.ultimate_parent_namespace_id,
            internal_projects.namespace_id
        from {{ ref("dim_issue") }}
        inner join
            internal_projects on internal_projects.project_id = dim_issue.dim_project_id

    ),
    notes as (

        select * from {{ ref("gitlab_dotcom_notes_source") }} where system = false

    ),
    awards as (select * from {{ ref("gitlab_dotcom_award_emoji_source") }}),
    internal_notes as (

        select
            coalesce(
                merge_requests.ultimate_parent_namespace_id,
                issues.ultimate_parent_namespace_id
            ) as ultimate_parent_namespace_id,
            coalesce(merge_requests.namespace_id, issues.namespace_id) as namespace_id,
            coalesce(merge_requests.project_id, issues.project_id) as project_id,
            noteable_type,
            merge_request_id,
            issue_id,
            note_id,
            note_author_id
        from notes
        left join
            merge_requests
            on merge_requests.merge_request_id = notes.noteable_id
            and notes.noteable_type = 'MergeRequest'
        left join
            issues
            on issues.issue_id = notes.noteable_id
            and notes.noteable_type = 'Issue'
        where
            (merge_requests.merge_request_id is not null or issues.issue_id is not null)

    ),
    internal_note_awards as (
        select
            internal_notes.*,
            awards.award_emoji_id,
            awards.award_emoji_name,
            awards.user_id as awarder_user_id
        from internal_notes
        left join
            awards
            on internal_notes.note_id = awards.awardable_id
            and awards.awardable_type = 'Note'

    )

select *
from internal_note_awards
