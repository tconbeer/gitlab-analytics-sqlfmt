{{ config(tags=["product"]) }}

{{ config({"materialized": "incremental", "unique_key": "dim_issue_id"}) }}

{{
    simple_cte(
        [
            ("dim_date", "dim_date"),
            ("dim_namespace_plan_hist", "dim_namespace_plan_hist"),
            ("plans", "gitlab_dotcom_plans_source"),
            ("prep_project", "prep_project"),
            ("prep_user", "prep_user"),
            ("prep_issue_severity", "prep_issue_severity"),
            ("prep_label_links", "prep_label_links"),
            ("prep_labels", "prep_labels"),
            ("gitlab_dotcom_epic_issues_source", "gitlab_dotcom_epic_issues_source"),
            ("gitlab_dotcom_routes_source", "gitlab_dotcom_routes_source"),
            ("gitlab_dotcom_projects_source", "gitlab_dotcom_projects_source"),
            ("gitlab_dotcom_milestones_source", "gitlab_dotcom_milestones_source"),
            ("gitlab_dotcom_award_emoji_source", "gitlab_dotcom_award_emoji_source"),
        ]
    )
}},
gitlab_dotcom_issues_source as (

    select *
    from {{ ref("gitlab_dotcom_issues_source") }}
    {% if is_incremental() %}

    where updated_at >= (select max(updated_at) from {{ this }})

    {% endif %}

),
upvote_count as (

    select
        awardable_id as dim_issue_id,
        sum(iff(award_emoji_name like 'thumbsup%', 1, 0)) as thumbsups_count,
        sum(iff(award_emoji_name like 'thumbsdown%', 1, 0)) as thumbsdowns_count,
        thumbsups_count - thumbsdowns_count as upvote_count
    from gitlab_dotcom_award_emoji_source
    where awardable_type = 'Issue'
    group by 1

),
agg_labels as (

    select
        gitlab_dotcom_issues_source.issue_id as dim_issue_id,
        array_agg(lower(prep_labels.label_title)) within group (
            order by prep_labels.label_title asc
        ) as labels
    from gitlab_dotcom_issues_source
    left join
        prep_label_links
        on gitlab_dotcom_issues_source.issue_id = prep_label_links.dim_issue_id
    left join prep_labels on prep_label_links.dim_label_id = prep_labels.dim_label_id
    group by gitlab_dotcom_issues_source.issue_id


),
renamed as (

    select
        gitlab_dotcom_issues_source.issue_id as dim_issue_id,

        -- FOREIGN KEYS
        gitlab_dotcom_issues_source.project_id as dim_project_id,
        prep_project.dim_namespace_id,
        prep_project.ultimate_parent_namespace_id,
        gitlab_dotcom_epic_issues_source.epic_id as dim_epic_id,
        dim_date.date_id as created_date_id,
        ifnull(dim_namespace_plan_hist.dim_plan_id, 34) as dim_plan_id,
        gitlab_dotcom_issues_source.author_id,
        gitlab_dotcom_issues_source.milestone_id,
        gitlab_dotcom_issues_source.sprint_id,

        gitlab_dotcom_issues_source.issue_iid as issue_internal_id,
        gitlab_dotcom_issues_source.updated_by_id,
        gitlab_dotcom_issues_source.last_edited_by_id,
        gitlab_dotcom_issues_source.moved_to_id,
        gitlab_dotcom_issues_source.created_at,
        gitlab_dotcom_issues_source.updated_at,
        gitlab_dotcom_issues_source.issue_last_edited_at,
        gitlab_dotcom_issues_source.issue_closed_at,
        gitlab_dotcom_issues_source.is_confidential,
        gitlab_dotcom_issues_source.issue_title,
        gitlab_dotcom_issues_source.issue_description,

        gitlab_dotcom_issues_source.weight,
        gitlab_dotcom_issues_source.due_date,
        gitlab_dotcom_issues_source.lock_version,
        gitlab_dotcom_issues_source.time_estimate,
        gitlab_dotcom_issues_source.has_discussion_locked,
        gitlab_dotcom_issues_source.closed_by_id,
        gitlab_dotcom_issues_source.relative_position,
        gitlab_dotcom_issues_source.service_desk_reply_to,
        gitlab_dotcom_issues_source.state_id,
        {{ map_state_id("state_id") }} as state_name,
        gitlab_dotcom_issues_source.duplicated_to_id,
        gitlab_dotcom_issues_source.promoted_to_epic_id,
        gitlab_dotcom_issues_source.issue_type,
        case
            when prep_issue_severity.severity = 4
            then 'S1'
            when
                array_contains('severity::1'::variant, agg_labels.labels)
                or array_contains('s1'::variant, agg_labels.labels)
            then 'S1'
            when prep_issue_severity.severity = 3
            then 'S2'
            when
                array_contains('severity::2'::variant, agg_labels.labels)
                or array_contains('s2'::variant, agg_labels.labels)
            then 'S2'
            when prep_issue_severity.severity = 2
            then 'S3'
            when
                array_contains('severity::3'::variant, agg_labels.labels)
                or array_contains('s3'::variant, agg_labels.labels)
            then 'S3'
            when prep_issue_severity.severity = 1
            then 'S4'
            when
                array_contains('severity::4'::variant, agg_labels.labels)
                or array_contains('s4'::variant, agg_labels.labels)
            then 'S4'
            else null
        end as severity,
        iff(
            gitlab_dotcom_projects_source.visibility_level = 'private',
            'private - masked',
            'https://gitlab.com/'
            || gitlab_dotcom_routes_source.path
            || '/issues/'
            || gitlab_dotcom_issues_source.issue_iid
        ) as issue_url,
        iff(
            gitlab_dotcom_projects_source.visibility_level = 'private',
            'private - masked',
            gitlab_dotcom_milestones_source.milestone_title
        ) as milestone_title,
        gitlab_dotcom_milestones_source.due_date as milestone_due_date,
        agg_labels.labels,
        ifnull(upvote_count.upvote_count, 0) as upvote_count
    from gitlab_dotcom_issues_source
    left join
        agg_labels on gitlab_dotcom_issues_source.issue_id = agg_labels.dim_issue_id
    left join
        prep_project
        on gitlab_dotcom_issues_source.project_id = prep_project.dim_project_id
    left join
        dim_namespace_plan_hist
        on prep_project.ultimate_parent_namespace_id
        = dim_namespace_plan_hist.dim_namespace_id
        and gitlab_dotcom_issues_source.created_at >= dim_namespace_plan_hist.valid_from
        and gitlab_dotcom_issues_source.created_at < coalesce(
            dim_namespace_plan_hist.valid_to, '2099-01-01'
        )
    left join
        dim_date on to_date(gitlab_dotcom_issues_source.created_at) = dim_date.date_day
    left join
        prep_issue_severity
        on gitlab_dotcom_issues_source.issue_id = prep_issue_severity.dim_issue_id
    left join
        gitlab_dotcom_epic_issues_source
        on gitlab_dotcom_issues_source.issue_id
        = gitlab_dotcom_epic_issues_source.issue_id
    left join
        gitlab_dotcom_projects_source
        on gitlab_dotcom_projects_source.project_id
        = gitlab_dotcom_issues_source.project_id
    left join
        gitlab_dotcom_routes_source
        on gitlab_dotcom_routes_source.source_id
        = gitlab_dotcom_issues_source.project_id
        and gitlab_dotcom_routes_source.source_type = 'Project'
    left join
        gitlab_dotcom_milestones_source
        on gitlab_dotcom_milestones_source.milestone_id
        = gitlab_dotcom_issues_source.milestone_id
    left join
        upvote_count on upvote_count.dim_issue_id = gitlab_dotcom_issues_source.issue_id
    where gitlab_dotcom_issues_source.project_id is not null
)

{{
    dbt_audit(
        cte_ref="renamed",
        created_by="@mpeychet_",
        updated_by="@jpeguero",
        created_date="2021-06-17",
        updated_date="2021-10-24",
    )
}}
