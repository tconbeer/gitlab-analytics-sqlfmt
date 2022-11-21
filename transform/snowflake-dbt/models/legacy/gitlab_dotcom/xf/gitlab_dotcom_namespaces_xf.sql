{{
    config(
        {
            "alias": "gitlab_dotcom_namespaces_xf",
            "post-hook": '{{ apply_dynamic_data_masking(columns = [{"namespace_id":"number"},{"namespace_name":"string"},{"namespace_path":"string"},{"owner_id":"number"},{"parent_id":"number"},{"push_rule_id":"number"},{"creator_id":"number"},{"namespace_ultimate_parent_id":"variant"},{"plan_id":"number"},{"plan_is_paid":"boolean"}]) }}',
        }
    )
}}

{% set fields_to_mask = ["namespace_name", "namespace_path"] %}

with
    namespaces as (select * from {{ ref("gitlab_dotcom_namespaces") }}),

    members as (

        select *
        from {{ ref("gitlab_dotcom_members") }} members
        where
            is_currently_valid = true
            and {{ filter_out_blocked_users("members", "user_id") }}

    ),

    projects as (select * from {{ ref("gitlab_dotcom_projects") }}),
    namespace_lineage as (select * from {{ ref("gitlab_dotcom_namespace_lineage") }}),
    creators as (

        select distinct author_id as creator_id, entity_id as group_id
        from {{ ref("prep_audit_event_details_clean") }} as audit_event_details_clean
        left join
            {{ ref("gitlab_dotcom_audit_events") }} as audit_events
            on audit_event_details_clean.audit_event_id = audit_events.audit_event_id
        where entity_type = 'Group' and key_name = 'add' and key_value = 'group'

    ),
    joined as (
        select
            namespaces.namespace_id,

            {% for field in fields_to_mask %}
            case
                when namespaces.visibility_level = 'public' or namespace_is_internal
                then {{ field }}
                when namespaces.visibility_level = 'internal'
                then 'internal - masked'
                when namespaces.visibility_level = 'private'
                then 'private - masked'
            end as {{ field }},
            {% endfor %}

            namespaces.owner_id,
            namespaces.namespace_type as namespace_type,
            namespaces.has_avatar,
            namespaces.created_at as namespace_created_at,
            namespaces.updated_at as namespace_updated_at,
            namespaces.is_membership_locked,
            namespaces.has_request_access_enabled,
            namespaces.has_share_with_group_locked,
            namespaces.visibility_level,
            namespaces.ldap_sync_status,
            namespaces.ldap_sync_error,
            namespaces.ldap_sync_last_update_at,
            namespaces.ldap_sync_last_successful_update_at,
            namespaces.ldap_sync_last_sync_at,
            namespaces.lfs_enabled,
            namespaces.parent_id,
            namespaces.shared_runners_enabled,
            namespaces.shared_runners_minutes_limit,
            namespaces.extra_shared_runners_minutes_limit,
            namespaces.repository_size_limit,
            namespaces.does_require_two_factor_authentication,
            namespaces.two_factor_grace_period,
            namespaces.project_creation_level,
            namespaces.push_rule_id,
            coalesce(creators.creator_id, namespaces.owner_id) as creator_id,

            namespace_lineage.namespace_is_internal,
            namespace_lineage.ultimate_parent_id as namespace_ultimate_parent_id,
            namespace_lineage.ultimate_parent_plan_id as plan_id,
            namespace_lineage.ultimate_parent_plan_title as plan_title,
            namespace_lineage.ultimate_parent_plan_is_paid as plan_is_paid,

            coalesce(count(distinct members.member_id), 0) as member_count,
            coalesce(count(distinct projects.project_id), 0) as project_count

        from namespaces
        left join
            members
            on namespaces.namespace_id = members.source_id
            and members.member_source_type = 'Namespace'
        left join projects on namespaces.namespace_id = projects.namespace_id
        left join
            namespace_lineage
            on namespaces.namespace_id = namespace_lineage.namespace_id
        left join
            creators on namespaces.namespace_id = creators.group_id
            {{ dbt_utils.group_by(n=33) }}
    )

select *
from joined
