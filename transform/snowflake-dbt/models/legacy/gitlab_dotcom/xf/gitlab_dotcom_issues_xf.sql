-- depends_on: {{ ref('engineering_productivity_metrics_projects_to_include') }}
-- depends_on: {{ ref('projects_part_of_product') }}
{% set fields_to_mask = ["issue_title", "issue_description"] %}


with
    issues as (

        select *
        from {{ ref("gitlab_dotcom_issues") }} issues
        where {{ filter_out_blocked_users("issues", "author_id") }}

    ),
    label_links as (

        select *
        from {{ ref("gitlab_dotcom_label_links") }}
        where is_currently_valid = true and target_type = 'Issue'

    ),
    all_labels as (select * from {{ ref("gitlab_dotcom_labels_xf") }}),
    derived_close_date as (

        select noteable_id as issue_id, created_at as derived_closed_at
        from {{ ref("gitlab_dotcom_notes") }}
        where
            noteable_type = 'Issue'
            and system = true
            and (contains(note, 'closed') or contains(note, 'moved to'))
        qualify
            row_number() over (partition by noteable_id order by created_at desc) = 1

    ),
    agg_labels as (

        select
            issues.issue_id,
            array_agg(
                lower(masked_label_title)) within group(order by masked_label_title asc
            ) as labels
        from issues
        left join label_links on issues.issue_id = label_links.target_id
        left join all_labels on label_links.label_id = all_labels.label_id
        group by issues.issue_id

    ),
    projects as (

        select project_id, namespace_id, visibility_level
        from {{ ref("gitlab_dotcom_projects") }}

    ),
    namespace_lineage as (select * from {{ ref("gitlab_dotcom_namespace_lineage") }}),
    gitlab_subscriptions as (

        select *
        from {{ ref("gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base") }}

    ),
    issue_metrics as (select * from {{ ref("gitlab_dotcom_issue_metrics") }}),
    events_weight as (

        select * from {{ ref("gitlab_dotcom_resource_weight_events_xf") }}

    ),
    first_events_weight as (

        select issue_id, min(created_at) first_weight_set_at
        from events_weight
        group by 1

    ),
    joined as (

        select
            issues.issue_id,
            issues.issue_iid,
            issues.author_id,
            issues.project_id,
            milestone_id,
            sprint_id,
            updated_by_id,
            last_edited_by_id,
            moved_to_id,
            issues.created_at as issue_created_at,
            issues.updated_at as issue_updated_at,
            issue_last_edited_at,
            -- issue_closed_at,
            iff(
                issue_closed_at is null and state = 'closed',
                derived_close_date.derived_closed_at,
                issue_closed_at
            ) as issue_closed_at,
            projects.namespace_id,
            visibility_level,
            is_confidential as issue_is_confidential,
            {% for field in fields_to_mask %}
            case
                when is_confidential = true
                then 'confidential - masked'
                when
                    visibility_level != 'public'
                    and namespace_lineage.namespace_is_internal = false
                then 'private/internal - masked'
                else {{ field }}
            end as {{ field }},
            {% endfor %}

            case
                when
                    projects.namespace_id = 9970
                    and array_contains(
                        'community contribution'::variant, agg_labels.labels
                    )
                then true
                else false
            end as is_community_contributor_related,

            case
                when
                    array_contains('severity::1'::variant, agg_labels.labels)
                    or array_contains('S1'::variant, agg_labels.labels)
                then 'severity 1'
                when
                    array_contains('severity::2'::variant, agg_labels.labels)
                    or array_contains('S2'::variant, agg_labels.labels)
                then 'severity 2'
                when
                    array_contains('severity::3'::variant, agg_labels.labels)
                    or array_contains('S3'::variant, agg_labels.labels)
                then 'severity 3'
                when
                    array_contains('severity::4'::variant, agg_labels.labels)
                    or array_contains('S4'::variant, agg_labels.labels)
                then 'severity 4'
                else 'undefined'
            end as severity_tag,

            case
                when
                    array_contains('priority::1'::variant, agg_labels.labels)
                    or array_contains('P1'::variant, agg_labels.labels)
                then 'priority 1'
                when
                    array_contains('priority::2'::variant, agg_labels.labels)
                    or array_contains('P2'::variant, agg_labels.labels)
                then 'priority 2'
                when
                    array_contains('priority::3'::variant, agg_labels.labels)
                    or array_contains('P3'::variant, agg_labels.labels)
                then 'priority 3'
                when
                    array_contains('priority::4'::variant, agg_labels.labels)
                    or array_contains('P4'::variant, agg_labels.labels)
                then 'priority 4'
                else 'undefined'
            end as priority_tag,

            case
                when
                    projects.namespace_id = 9970
                    and array_contains('security'::variant, agg_labels.labels)
                then true
                else false
            end as is_security_issue,

            iff(
                issues.project_id in (
                    {{ is_project_included_in_engineering_metrics() }}
                ),
                true,
                false
            ) as is_included_in_engineering_metrics,
            iff(
                issues.project_id in ({{ is_project_part_of_product() }}), true, false
            ) as is_part_of_product,
            state,
            weight,
            due_date,
            lock_version,
            time_estimate,
            has_discussion_locked,
            closed_by_id,
            relative_position,
            service_desk_reply_to,
            duplicated_to_id,
            promoted_to_epic_id,
            issue_type,

            agg_labels.labels,
            array_to_string(agg_labels.labels, '|') as masked_label_title,

            namespace_lineage.namespace_is_internal as is_internal_issue,
            namespace_lineage.ultimate_parent_id,
            namespace_lineage.ultimate_parent_plan_id,
            namespace_lineage.ultimate_parent_plan_title,
            namespace_lineage.ultimate_parent_plan_is_paid,

            case
                when gitlab_subscriptions.is_trial
                then 'trial'
                else coalesce(gitlab_subscriptions.plan_id, 34)::varchar
            end as plan_id_at_issue_creation,

            issue_metrics.first_mentioned_in_commit_at,
            issue_metrics.first_associated_with_milestone_at,
            issue_metrics.first_added_to_board_at,
            first_events_weight.first_weight_set_at

        from issues
        left join agg_labels on issues.issue_id = agg_labels.issue_id
        left join projects on issues.project_id = projects.project_id
        left join
            namespace_lineage on projects.namespace_id = namespace_lineage.namespace_id
        left join
            gitlab_subscriptions
            on namespace_lineage.ultimate_parent_id = gitlab_subscriptions.namespace_id
            and issues.created_at between gitlab_subscriptions.valid_from and
            {{ coalesce_to_infinity("gitlab_subscriptions.valid_to") }}
        left join issue_metrics on issues.issue_id = issue_metrics.issue_id
        left join first_events_weight on issues.issue_id = first_events_weight.issue_id
        left join derived_close_date on issues.issue_id = derived_close_date.issue_id
    )

select *
from joined
