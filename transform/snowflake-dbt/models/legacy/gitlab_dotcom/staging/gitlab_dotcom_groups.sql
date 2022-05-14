{{ config({
    "database": env_var('SNOWFLAKE_PROD_DATABASE') ,
    "schema": "legacy"
    })
}}
with
    namespace_groups as (

        select
            namespace_id as group_id,
            namespace_name as group_name,
            namespace_path as group_path,
            owner_id,
            has_avatar,
            created_at,
            updated_at,
            is_membership_locked,
            has_request_access_enabled,
            has_share_with_group_locked,
            visibility_level,
            ldap_sync_status,
            ldap_sync_error,
            ldap_sync_last_update_at,
            ldap_sync_last_successful_update_at,
            ldap_sync_last_sync_at,
            lfs_enabled,
            parent_id as parent_group_id,
            shared_runners_minutes_limit,
            repository_size_limit,
            does_require_two_factor_authentication,
            two_factor_grace_period,
            plan_id,
            project_creation_level,
            iff(namespaces.parent_id is not null, true, false) as is_parent_group

        from {{ ref("gitlab_dotcom_namespaces") }} as namespaces
        where namespace_type = 'Group'

    )

select *
from namespace_groups
