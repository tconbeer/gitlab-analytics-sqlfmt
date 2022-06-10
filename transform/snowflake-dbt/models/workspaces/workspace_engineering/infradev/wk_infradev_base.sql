{{
    simple_cte(
        [
            ("issues", "infradev_issues_base"),
            ("projects", "dim_project"),
            ("assigend_users", "infradev_current_issue_users"),
            ("label_groups", "infradev_label_history"),
            ("namespace_path", "infradev_namespace_path"),
        ]
    )
}}

,
dates as (

    select *, min(date_id) over () as min_date_id
    from {{ ref("dim_date") }}
    where
        date_actual > date_trunc(
            'month', dateadd('year', -2, current_date())
        ) and date_actual < current_date()
)

select
    {{ dbt_utils.surrogate_key(["dates.date_actual", "issues.dim_issue_id"]) }}
    as daily_issue_id,
    dates.date_actual,
    issues.dim_issue_id,
    issues.issue_internal_id,
    issues.dim_project_id,
    issues.dim_namespace_id,
    issues.labels,
    issues.issue_title,
    namespace_path.full_namespace_path,
    '[' || replace (
        replace (left(issues.issue_title, 64), '[', ''), ']', ''
    ) || '](https://gitlab.com/'
    ||
    namespace_path.full_namespace_path
    || '/'
    || projects.project_path
    || '/issues/'
    || issues.issue_internal_id
    ||
    ')'
    as issue_url,
    iff(dates.date_actual > issues.issue_closed_at, 'closed', 'open') as issue_state,
    issues.created_at as issue_created_at,
    issues.issue_closed_at,
    ifnull(label_groups.severity, 'No Severity') as severity,
    label_groups.severity_label_added_at,
    ifnull(label_groups.assigned_team, 'Unassigned') as assigned_team,
    label_groups.team_label_added_at,
    label_groups.team_label,
    iff(
        dates.date_actual > issues.issue_closed_at,
        null,
        datediff('day', issues.created_at, dates.date_actual)
    ) as issue_open_age_in_days,
    datediff(
        'day', label_groups.severity_label_added_at, dates.date_actual
    ) as severity_label_age_in_days,
    assigend_users.assigned_usernames,
    iff(assigend_users.assigned_usernames is null, true, false) as is_issue_unassigned
from issues
inner join
    dates on issues.created_date_id <= dates.date_id and (
        issues.created_date_id > dates.min_date_id or issues.created_date_id is null
    )
left join projects on issues.dim_project_id = projects.dim_project_id
left join namespace_path on issues.dim_namespace_id = namespace_path.dim_namespace_id
left join assigend_users on issues.dim_issue_id = assigend_users.dim_issue_id
left join
    label_groups
    on issues.dim_issue_id = label_groups.dim_issue_id
    and dates.date_actual between date_trunc(
        'day', label_groups.label_group_valid_from
    ) and date_trunc('day', label_groups.label_group_valid_to)
