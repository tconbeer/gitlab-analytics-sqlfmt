{{ config(tags=["mnpi_exception"]) }}

{{
    config(
        {
            "materialized": "incremental",
            "unique_key": "event_primary_key",
            "automatic_clustering": true,
        }
    )
}}

/*
  Each dict must have ALL of the following:
    * event_name
    * primary_key
    * stage_name": "create",
    * "is_representative_of_stage
    * primary_key"
  Must have ONE of the following:
    * source_cte_name OR source_table_name
    * key_to_parent_project OR key_to_group_project (NOT both, see how clusters_applications_helm is included twice for group and project.
*/
{%- set event_ctes = [
    {
        "event_name": "incident_labeled_issues",
        "source_cte_name": "incident_labeled_issues_source",
        "user_column_name": "author_id",
        "key_to_parent_project": "project_id",
        "primary_key": "issue_id",
        "stage_name": "monitor",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "issues",
        "source_table_name": "gitlab_dotcom_issues",
        "user_column_name": "author_id",
        "key_to_parent_project": "project_id",
        "primary_key": "issue_id",
        "stage_name": "plan",
        "is_representative_of_stage": "True",
    },
    {
        "event_name": "issue_resource_label_events",
        "source_cte_name": "issue_resource_label_events_source",
        "user_column_name": "user_id",
        "key_to_parent_project": "namespace_id",
        "primary_key": "resource_label_event_id",
        "stage_name": "plan",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "issue_resource_weight_events",
        "source_table_name": "gitlab_dotcom_resource_weight_events_xf",
        "user_column_name": "user_id",
        "key_to_parent_project": "project_id",
        "primary_key": "resource_weight_event_id",
        "stage_name": "plan",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "issue_resource_milestone_events",
        "source_cte_name": "issue_resource_milestone_events_source",
        "user_column_name": "user_id",
        "key_to_parent_project": "project_id",
        "primary_key": "resource_milestone_event_id",
        "stage_name": "plan",
        "is_representative_of_stage": "False",
    },
] -%}


{{
    simple_cte(
        [
            (
                "gitlab_subscriptions",
                "gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base",
            ),
            ("namespaces", "gitlab_dotcom_namespaces_xf"),
            ("plans", "gitlab_dotcom_plans"),
            ("projects", "gitlab_dotcom_projects_xf"),
            ("users", "gitlab_dotcom_users"),
            ("blocked_users", "gitlab_dotcom_users_blocked_xf"),
        ]
    )
}}


/* Source CTEs Start Here */
,
incident_labeled_issues_source as (

    select *, issue_created_at as created_at
    from {{ ref("gitlab_dotcom_issues_xf") }}
    where array_contains('incident'::variant, labels)

),
issue_resource_label_events_source as (

    select *
    from {{ ref("gitlab_dotcom_resource_label_events_xf") }}
    where issue_id is not null

),
issue_resource_milestone_events_source as (

    select *
    from {{ ref("gitlab_dotcom_resource_milestone_events_xf") }}
    where issue_id is not null

)
/* End of Source CTEs */
{% for event_cte in event_ctes %}

,
{{ event_cte.event_name }} as (

    select
        *,
        md5(
            {{ event_cte.primary_key }} || '-' || '{{ event_cte.event_name }}'
        ) as event_primary_key
    /* Check for source_table_name, else use source_cte_name. */
    {% if event_cte.source_table_name is defined %}
    from {{ ref(event_cte.source_table_name) }}
    {% else %} from {{ event_cte.source_cte_name }}
    {% endif %}
    where
        created_at is not null and created_at >= dateadd(month, -25, current_date)

        {% if is_incremental() %}

        and created_at >= (
            select max(event_created_at)
            from {{ this }}
            where event_name = '{{ event_cte.event_name }}'
        )

        {% endif %}

)

{% endfor -%}

,
data as (

    {% for event_cte in event_ctes %}

    select
        event_primary_key,
        '{{ event_cte.event_name }}' as event_name,
        ultimate_namespace.namespace_id,
        ultimate_namespace.namespace_created_at,
        iff(blocked_users.user_id is not null, true, false) as is_blocked_namespace,
        {% if "NULL" in event_cte.user_column_name %} null
        {% else %} {{ event_cte.event_name }}.{{ event_cte.user_column_name }}
        {% endif %} as user_id,
        {% if event_cte.key_to_parent_project is defined %}
        'project' as parent_type,
        projects.project_id as parent_id,
        projects.project_created_at as parent_created_at,
        projects.is_learn_gitlab as project_is_learn_gitlab,
        {% elif event_cte.key_to_parent_group is defined %}
        'group' as parent_type,
        namespaces.namespace_id as parent_id,
        namespaces.namespace_created_at as parent_created_at,
        null as project_is_learn_gitlab,
        {% else %}
        null as parent_type,
        null as parent_id,
        null as parent_created_at,
        null as project_is_learn_gitlab,
        {% endif %}
        ultimate_namespace.namespace_is_internal as namespace_is_internal,
        {{ event_cte.event_name }}.created_at as event_created_at,
        {{ event_cte.is_representative_of_stage }}::boolean
        as is_representative_of_stage,
        '{{ event_cte.stage_name }}' as stage_name,
        case
            when gitlab_subscriptions.is_trial
            then 'trial'
            else coalesce(gitlab_subscriptions.plan_id, 34)::varchar
        end as plan_id_at_event_date,
        case
            when gitlab_subscriptions.is_trial
            then 'trial'
            else coalesce(plans.plan_name, 'free')
        end as plan_name_at_event_date,
        coalesce(plans.plan_is_paid, false) as plan_was_paid_at_event_date
    from {{ event_cte.event_name }}
    /* Join with parent project. */
    {% if event_cte.key_to_parent_project is defined %}
    left join
        projects
        on {{ event_cte.event_name }}.{{ event_cte.key_to_parent_project }}
        = projects.project_id
    /* Join with parent group. */
    {% elif event_cte.key_to_parent_group is defined %}
    left join
        namespaces
        on {{ event_cte.event_name }}.{{ event_cte.key_to_parent_group }}
        = namespaces.namespace_id
    {% endif %}

    -- Join on either the project's or the group's ultimate namespace.
    left join
        namespaces as ultimate_namespace
        {% if event_cte.key_to_parent_project is defined %}
        on ultimate_namespace.namespace_id = projects.ultimate_parent_id
        {% elif event_cte.key_to_parent_group is defined %}
        on ultimate_namespace.namespace_id = namespaces.namespace_ultimate_parent_id
        {% else %} on false  -- Don't join any rows.
        {% endif %}

    left join
        gitlab_subscriptions
        on ultimate_namespace.namespace_id = gitlab_subscriptions.namespace_id
        and {{ event_cte.event_name }}.created_at >= to_date(
            gitlab_subscriptions.valid_from
        )
        and {{ event_cte.event_name }}.created_at
        < {{ coalesce_to_infinity("TO_DATE(gitlab_subscriptions.valid_to)") }}
    left join plans on gitlab_subscriptions.plan_id = plans.plan_id
    left join blocked_users on ultimate_namespace.creator_id = blocked_users.user_id
    {% if "NULL" not in event_cte.user_column_name %}
    where
        {{ filter_out_blocked_users(event_cte.event_name, event_cte.user_column_name) }}
    {% endif %}


    {% if not loop.last %}
    union
    {% endif %}
    {% endfor -%}

)

,
final as (
    select
        data.*,
        users.created_at as user_created_at,
        floor(
            datediff('hour', namespace_created_at, event_created_at) / 24
        ) as days_since_namespace_creation,
        floor(
            datediff('hour', namespace_created_at, event_created_at) / (24 * 7)
        ) as weeks_since_namespace_creation,
        floor(
            datediff('hour', parent_created_at, event_created_at) / 24
        ) as days_since_parent_creation,
        floor(
            datediff('hour', parent_created_at, event_created_at) / (24 * 7)
        ) as weeks_since_parent_creation,
        floor(
            datediff('hour', user_created_at, event_created_at) / 24
        ) as days_since_user_creation,
        floor(
            datediff('hour', user_created_at, event_created_at) / (24 * 7)
        ) as weeks_since_user_creation
    from data
    left join users on data.user_id = users.user_id
    where event_created_at < current_date()

)

select *
from final
