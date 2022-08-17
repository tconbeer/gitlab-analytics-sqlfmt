-- depends_on: {{ ref('engineering_productivity_metrics_projects_to_include') }}
-- depends_on: {{ ref('projects_part_of_product') }}
with
    merge_requests as (

        select
            {{
                dbt_utils.star(
                    from=ref("gitlab_dotcom_merge_requests"),
                    except=["created_at", "updated_at"],
                )
            }},
            created_at as merge_request_created_at,
            updated_at as merge_request_updated_at
        from {{ ref("gitlab_dotcom_merge_requests") }} merge_requests
        where {{ filter_out_blocked_users("merge_requests", "author_id") }}

    ),
    label_links as (

        select *
        from {{ ref("gitlab_dotcom_label_links") }}
        where is_currently_valid = true and target_type = 'MergeRequest'

    ),
    all_labels as (select * from {{ ref("gitlab_dotcom_labels_xf") }}),
    agg_labels as (

        select
            merge_requests.merge_request_id,
            array_agg(lower(masked_label_title)) within group (
                order by masked_label_title asc
            ) as labels
        from merge_requests
        left join label_links on merge_requests.merge_request_id = label_links.target_id
        left join all_labels on label_links.label_id = all_labels.label_id
        group by merge_requests.merge_request_id

    ),
    latest_merge_request_metric as (

        select max(merge_request_metric_id) as target_id
        from {{ ref("gitlab_dotcom_merge_request_metrics") }}
        group by merge_request_id

    ),
    merge_request_metrics as (

        select *
        from {{ ref("gitlab_dotcom_merge_request_metrics") }}
        inner join latest_merge_request_metric on merge_request_metric_id = target_id

    ),
    milestones as (select * from {{ ref("gitlab_dotcom_milestones") }}),
    projects as (select * from {{ ref("gitlab_dotcom_projects_xf") }}),
    author_namespaces as (

        select *
        from {{ ref("gitlab_dotcom_namespaces_xf") }}
        where namespace_type = 'User'

    ),
    gitlab_subscriptions as (

        select *
        from {{ ref("gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base") }}

    ),
    joined as (

        select
            merge_requests.*,
            iff(
                projects.visibility_level != 'public'
                and projects.namespace_is_internal = false,
                'content masked',
                milestones.milestone_title
            ) as milestone_title,
            iff(
                projects.visibility_level != 'public'
                and projects.namespace_is_internal = false,
                'content masked',
                milestones.milestone_description
            ) as milestone_description,
            projects.namespace_id,
            projects.ultimate_parent_id,
            projects.ultimate_parent_plan_id,
            projects.ultimate_parent_plan_title,
            projects.ultimate_parent_plan_is_paid,
            projects.namespace_is_internal,
            author_namespaces.namespace_path as author_namespace_path,
            array_to_string(agg_labels.labels, '|') as masked_label_title,
            agg_labels.labels,
            merge_request_metrics.merged_at,
            iff(
                merge_requests.target_project_id in (
                    {{ is_project_included_in_engineering_metrics() }}
                ),
                true,
                false
            ) as is_included_in_engineering_metrics,
            iff(
                merge_requests.target_project_id in (
                    {{ is_project_part_of_product() }}
                ),
                true,
                false
            ) as is_part_of_product,
            iff(
                projects.namespace_is_internal is not null
                and array_contains(
                    'community contribution'::variant, agg_labels.labels
                ),
                true,
                false
            ) as is_community_contributor_related,
            timestampdiff(
                hours,
                merge_requests.merge_request_created_at,
                merge_request_metrics.merged_at
            ) as hours_to_merged_status,
            regexp_count(
                merge_requests.merge_request_description,
                '([-+*]|[\d+\.]) [\[]( |[xX])[\]]',
                1,
                'm'
            ) as total_checkboxes,
            regexp_count(
                merge_requests.merge_request_description,
                '([-+*]|[\d+\.]) [\[][xX][\]]',
                1,
                'm'
            ) as completed_checkboxes,
            -- Original regex,
            -- (?:(?:>\s{0,4})*)(?:\s*(?:[-+*]|(?:\d+\.)))+\s+(\[\s\]|\[[xX]\])(\s.+),
            -- found in
            -- https://gitlab.com/gitlab-org/gitlab/-/blob/master/app/models/concerns/taskable.rb
            case
                when gitlab_subscriptions.is_trial
                then 'trial'
                else coalesce(gitlab_subscriptions.plan_id, 34)::varchar
            end as plan_id_at_merge_request_creation

        from merge_requests
        left join
            agg_labels on merge_requests.merge_request_id = agg_labels.merge_request_id
        left join
            merge_request_metrics
            on merge_requests.merge_request_id = merge_request_metrics.merge_request_id
        left join milestones on merge_requests.milestone_id = milestones.milestone_id
        left join projects on merge_requests.target_project_id = projects.project_id
        left join
            author_namespaces on merge_requests.author_id = author_namespaces.owner_id
        left join
            gitlab_subscriptions
            on projects.ultimate_parent_id = gitlab_subscriptions.namespace_id
            and merge_requests.created_at
            between gitlab_subscriptions.valid_from
            and {{ coalesce_to_infinity("gitlab_subscriptions.valid_to") }}

    )

select *
from joined
