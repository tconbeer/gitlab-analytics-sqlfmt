{% set fields_to_mask = ["epic_title", "epic_description"] %}

/* Code is sourced from gitlab_dotcom_issues_xf */
with
    epics as (select * from {{ ref("gitlab_dotcom_epics") }}),
    label_links as (

        select *
        from {{ ref("gitlab_dotcom_label_links") }}
        where is_currently_valid = true and target_type = 'Epic'

    ),
    all_labels as (select * from {{ ref("gitlab_dotcom_labels_xf") }}),
    agg_labels as (

        select
            epics.epic_id,
            array_agg(lower(masked_label_title)) within group(
                order by masked_label_title asc
            ) as labels
        from epics
        left join label_links on epics.epic_id = label_links.target_id
        left join all_labels on label_links.label_id = all_labels.label_id
        group by epics.epic_id

    ),
    namespaces as (select * from {{ ref("gitlab_dotcom_namespaces") }}),
    namespace_lineage as (select * from {{ ref("gitlab_dotcom_namespace_lineage") }}),
    gitlab_subscriptions as (

        select *
        from {{ ref("gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base") }}
    ),

    joined as (

        select
            {{ dbt_utils.star(from=ref('gitlab_dotcom_epics'), except=fields_to_mask|upper, relation_alias='epics') }},

            {% for field in fields_to_mask %}
            case
                when {{ field }} = null
                then null
                when namespaces.visibility_level = 'public'
                then {{ field }}
                when namespace_lineage.namespace_is_internal = true
                then {{ field }}
                else 'private/internal - masked'
            end as {{ field }},
            {% endfor %}

            agg_labels.labels,

            namespaces.visibility_level as namespace_visibility_level,
            namespace_lineage.namespace_is_internal as is_internal_epic,
            namespace_lineage.ultimate_parent_id,
            namespace_lineage.ultimate_parent_plan_id,
            namespace_lineage.ultimate_parent_plan_title,
            namespace_lineage.ultimate_parent_plan_is_paid,

            case
                when gitlab_subscriptions.is_trial
                then 'trial'
                else coalesce(gitlab_subscriptions.plan_id, 34)::varchar
            end as plan_id_at_epic_creation

        from epics
        left join agg_labels on epics.epic_id = agg_labels.epic_id
        left join namespaces on epics.group_id = namespaces.namespace_id
        left join namespace_lineage on epics.group_id = namespace_lineage.namespace_id
        left join
            gitlab_subscriptions
            on namespace_lineage.ultimate_parent_id = gitlab_subscriptions.namespace_id
            and epics.created_at between gitlab_subscriptions.valid_from and {{
                coalesce_to_infinity(
                    "gitlab_subscriptions.valid_to"
                )
            }}
    )

select *
from joined
