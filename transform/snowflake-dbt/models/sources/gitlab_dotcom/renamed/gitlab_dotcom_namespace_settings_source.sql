with
    source as (

        select * from {{ ref("gitlab_dotcom_namespace_settings_dedupe_source") }}

    ),
    renamed as (

        select
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            namespace_id::number as namespace_id,
            prevent_forking_outside_group::boolean as prevent_forking_outside_group,
            allow_mfa_for_subgroups::boolean as allow_mfa_for_subgroups,
            default_branch_name::varchar as default_branch_name,
            repository_read_only::boolean as repository_read_only,
            delayed_project_removal::boolean as delayed_project_removal,
            resource_access_token_creation_allowed::boolean
            as resource_access_token_creation_allowed,
            lock_delayed_project_removal::boolean as lock_delayed_project_removal,
            prevent_sharing_groups_outside_hierarchy::boolean
            as prevent_sharing_groups_outside_hierarchy,
            new_user_signups_cap::number as new_signups_cap,
            setup_for_company::boolean as is_setup_for_company,
            jobs_to_be_done::number as jobs_to_be_done
        from source

    )

select *
from renamed
