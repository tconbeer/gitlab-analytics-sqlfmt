{% set fields_to_mask = ["milestone_description", "milestone_title"] %}

with
    milestones as (select * from {{ ref("gitlab_dotcom_milestones") }}),

    -- A milestone joins to a namespace through EITHER a project or group
    projects as (select * from {{ ref("gitlab_dotcom_projects") }}),

    internal_namespaces as (

        select namespace_id
        from {{ ref("gitlab_dotcom_namespace_lineage") }}
        where namespace_is_internal = true
    ),

    final as (

        select
            milestones.milestone_id,

            {% for field in fields_to_mask %}
            iff(
                internal_namespaces.namespace_id is null,
                'private - masked',
                {{ field }}
            ) as {{ field }},
            {% endfor %}

            milestones.due_date,
            milestones.group_id,
            milestones.created_at as milestone_created_at,
            milestones.updated_at as milestone_updated_at,
            milestones.milestone_status,
            coalesce(milestones.group_id, projects.namespace_id) as namespace_id,
            milestones.project_id,
            milestones.start_date

        from milestones
        left join projects on milestones.project_id = projects.project_id
        left join
            internal_namespaces
            on projects.namespace_id = internal_namespaces.namespace_id
            or milestones.group_id = internal_namespaces.namespace_id
    )

select *
from final
