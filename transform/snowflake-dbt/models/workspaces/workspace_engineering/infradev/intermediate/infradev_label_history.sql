{{ config(materialized="ephemeral") }}

with
    labels as (select * from {{ ref("prep_labels") }}),
    label_links as (select * from {{ ref("gitlab_dotcom_label_links_source") }}),
    label_type as (

        select
            dim_label_id,
            label_title,
            case
                when
                    lower(label_title) in (
                        'severity::1', 'severity::2', 'severity::3', 'severity::4'
                    )
                then 'severity'
                when lower(label_title) like 'team%'
                then 'team'
                when lower(label_title) like 'group%'
                then 'team'
                else 'other'
            end as label_type
        from labels

    ),
    base_labels as (

        select
            label_links.label_link_id as dim_issue_id,
            label_type.label_title,
            label_type.label_type,
            label_links.label_link_created_at as label_added_at,
            label_links.label_link_created_at as label_valid_from,
            lead(label_links.label_link_created_at, 1, current_date()) over (
                partition by label_links.label_link_id, label_type.label_type
                order by label_links.label_link_created_at
            ) as label_valid_to
        from label_type
        left join
            label_links
            on label_type.dim_label_id = label_links.target_id
            and label_links.target_type = 'Issue'
        where label_type.label_type != 'other' and label_links.label_link_id is not null

    ),
    label_groups as (

        select
            severity.dim_issue_id,
            severity.label_title as severity_label,
            team.label_title as team_label,
            'S' || right(severity_label, 1) as severity,
            split(team_label, '::') [array_size(split(team_label, '::')) - 1]::varchar
            as assigned_team,
            severity.label_added_at as severity_label_added_at,
            team.label_added_at as team_label_added_at,
            greatest(
                severity.label_valid_from, team.label_valid_from
            ) as label_group_valid_from,
            least(severity.label_valid_to, team.label_valid_to) as label_group_valid_to
        from base_labels as severity
        inner join base_labels as team on severity.dim_issue_id = team.dim_issue_id
        where severity.label_type = 'severity' and team.label_type = 'team'

    )

select *
from label_groups
