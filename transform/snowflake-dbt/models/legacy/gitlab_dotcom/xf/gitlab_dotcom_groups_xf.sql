{% set fields_to_mask = ["group_name", "group_path"] %}

with
    groups as (select * from {{ ref("gitlab_dotcom_groups") }}),

    members as (

        select *
        from {{ ref("gitlab_dotcom_members") }} members
        where
            is_currently_valid = true
            and {{ filter_out_blocked_users("members", "user_id") }}

    ),

    projects as (select * from {{ ref("gitlab_dotcom_projects") }}),
    namespace_lineage as (select * from {{ ref("gitlab_dotcom_namespace_lineage") }}),
    joined as (

        select
            groups.group_id,

            {% for field in fields_to_mask %}
            case
                when groups.visibility_level = 'public' or namespace_is_internal
                then groups.{{ field }}
                when groups.visibility_level = 'internal' and not namespace_is_internal
                then 'internal - masked'
                when groups.visibility_level = 'private' and not namespace_is_internal
                then 'private - masked'
            end as {{ field }},
            {% endfor %}

            groups.owner_id,
            groups.has_avatar,
            groups.created_at as group_created_at,
            groups.updated_at as group_updated_at,
            groups.is_membership_locked,
            groups.has_request_access_enabled,
            groups.has_share_with_group_locked,
            groups.visibility_level,
            groups.ldap_sync_status,
            groups.ldap_sync_error,
            groups.ldap_sync_last_update_at,
            groups.ldap_sync_last_successful_update_at,
            groups.ldap_sync_last_sync_at,
            groups.lfs_enabled,
            groups.parent_group_id,
            iff(groups.parent_group_id is null, true, false) as is_top_level_group,
            groups.shared_runners_minutes_limit,
            groups.repository_size_limit,
            groups.does_require_two_factor_authentication,
            groups.two_factor_grace_period,
            groups.project_creation_level,

            namespace_lineage.namespace_is_internal as group_is_internal,
            namespace_lineage.ultimate_parent_id as group_ultimate_parent_id,
            namespace_lineage.ultimate_parent_plan_id as group_plan_id,
            namespace_lineage.ultimate_parent_plan_title as group_plan_title,
            namespace_lineage.ultimate_parent_plan_is_paid as group_plan_is_paid,

            coalesce(count(distinct members.member_id), 0) as member_count,
            coalesce(count(distinct projects.project_id), 0) as project_count

        from groups
        left join
            members
            on groups.group_id = members.source_id
            and members.member_source_type = 'Namespace'
        left join projects on projects.namespace_id = groups.group_id
        left join
            namespace_lineage on groups.group_id = namespace_lineage.namespace_id
            {{ dbt_utils.group_by(n=29) }}

    )

select *
from joined
