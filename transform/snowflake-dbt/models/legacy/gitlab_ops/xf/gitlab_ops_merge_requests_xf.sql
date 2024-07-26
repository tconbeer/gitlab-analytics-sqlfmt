-- depends_on: {{ ref('projects_part_of_product_ops') }}
-- depends_on: {{ ref('engineering_productivity_metrics_projects_to_include') }}
-- These data models are required for this data model based on
-- https://gitlab.com/gitlab-data/analytics/-/blob/master/transform/snowflake-dbt/models/staging/gitlab_ops/xf/gitlab_ops_merge_requests_xf.sql
-- This data model is missing a lot of other source data models
with
    merge_requests as (

        select
            {{
                dbt_utils.star(
                    from=ref("gitlab_ops_merge_requests"),
                    except=["created_at", "updated_at"],
                )
            }},
            created_at as merge_request_created_at,
            updated_at as merge_request_updated_at
        from {{ ref("gitlab_ops_merge_requests") }} merge_requests

    ),
    label_links as (

        select *
        from {{ ref("gitlab_ops_label_links") }}
        where is_currently_valid = true and target_type = 'MergeRequest'

    ),
    all_labels as (select * from {{ ref("gitlab_ops_labels_xf") }}),
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
        from {{ ref("gitlab_ops_merge_request_metrics") }}
        inner join latest_merge_request_metric on merge_request_metric_id = target_id

    ),
    projects as (select * from {{ ref("gitlab_ops_projects_xf") }}),
    joined as (

        select
            merge_requests.*,
            merge_request_metrics.merged_at,
            projects.namespace_id,
            array_to_string(agg_labels.labels, '|') as masked_label_title,
            agg_labels.labels,
            iff(
                merge_requests.target_project_id
                in ({{ is_project_included_in_engineering_metrics() }}),
                true,
                false
            ) as is_included_in_engineering_metrics,
            iff(
                merge_requests.target_project_id
                in ({{ is_project_part_of_product_ops() }}),
                true,
                false
            ) as is_part_of_product_ops
        from merge_requests
        left join
            merge_request_metrics
            on merge_requests.merge_request_id = merge_request_metrics.merge_request_id
        left join
            agg_labels on merge_requests.merge_request_id = agg_labels.merge_request_id
        left join projects on merge_requests.target_project_id = projects.project_id

    )

select *
from joined
