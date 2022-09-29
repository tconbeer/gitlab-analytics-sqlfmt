
with
    source as (select * from {{ ref("gitlab_dotcom_namespaces_dedupe_source") }}),
    renamed as (

        select
            id::number as namespace_id,
            name::varchar as namespace_name,
            path::varchar as namespace_path,
            owner_id::number as owner_id,
            type as namespace_type,
            iff(avatar is null, false, true) as has_avatar,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            membership_lock::boolean as is_membership_locked,
            request_access_enabled::boolean as has_request_access_enabled,
            share_with_group_lock::boolean as has_share_with_group_locked,
            case
                when visibility_level = '20'
                then 'public'
                when visibility_level = '10'
                then 'internal'
                else 'private'
            end::varchar as visibility_level,
            ldap_sync_status,
            ldap_sync_error,
            ldap_sync_last_update_at::timestamp as ldap_sync_last_update_at,
            ldap_sync_last_successful_update_at::timestamp
            as ldap_sync_last_successful_update_at,
            ldap_sync_last_sync_at::timestamp as ldap_sync_last_sync_at,
            lfs_enabled::boolean as lfs_enabled,
            parent_id::number as parent_id,
            shared_runners_minutes_limit::number as shared_runners_minutes_limit,
            extra_shared_runners_minutes_limit::number
            as extra_shared_runners_minutes_limit,
            repository_size_limit::number as repository_size_limit,
            require_two_factor_authentication::boolean
            as does_require_two_factor_authentication,
            two_factor_grace_period::number as two_factor_grace_period,
            project_creation_level::number as project_creation_level,
            push_rule_id::integer as push_rule_id,
            shared_runners_enabled:boolean as shared_runners_enabled
        from source

    )

select *
from renamed
