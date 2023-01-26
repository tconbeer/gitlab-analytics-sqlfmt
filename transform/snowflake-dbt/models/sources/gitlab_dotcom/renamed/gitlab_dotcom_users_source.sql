with
    source as (select * from {{ ref("gitlab_dotcom_users_dedupe_source") }}),

    renamed as (

        select
            id::number as user_id,
            remember_created_at::timestamp as remember_created_at,
            sign_in_count::number as sign_in_count,
            current_sign_in_at::timestamp as current_sign_in_at,
            last_sign_in_at::timestamp as last_sign_in_at,
            -- current_sign_in_ip   // hidden for privacy
            -- last_sign_in_ip   // hidden for privacy
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            admin::boolean as is_admin,
            projects_limit::number as projects_limit,
            failed_attempts::number as failed_attempts,
            locked_at::timestamp as locked_at,
            iff(lower(locked_at) = 'nan', false, true) as user_locked,
            can_create_group::boolean as has_create_group_permissions,
            can_create_team::boolean as has_create_team_permissions,
            state::varchar as state,
            color_scheme_id::number as color_scheme_id,
            password_expires_at::timestamp as password_expires_at,
            created_by_id::number as created_by_id,
            last_credential_check_at::timestamp as last_credential_check_at,
            iff(lower(avatar) = 'nan', false, true) as has_avatar,
            confirmed_at::timestamp as confirmed_at,
            confirmation_sent_at::timestamp as confirmation_sent_at,
            -- unconfirmed_email // hidden for privacy
            hide_no_ssh_key::boolean as has_hide_no_ssh_key_enabled,
            -- website_url // hidden for privacy
            admin_email_unsubscribed_at::timestamp as admin_email_unsubscribed_at,
            -- Coalesced to match application behavior
            -- https://gitlab.com/gitlab-data/analytics/-/issues/12046#note_863577705
            coalesce(notification_email, email)::varchar as notification_email,
            split_part(
                coalesce(notification_email, email), '@', 2
            ) as notification_email_domain,
            hide_no_password::boolean as has_hide_no_password_enabled,
            password_automatically_set::boolean as is_password_automatically_set,
            iff(lower(location) = 'nan', null, location)
            as location,  -- noqa:L029
            email::varchar as email,
            split_part(email, '@', 2) as email_domain,
            public_email::varchar as public_email,
            split_part(public_email, '@', 2) as public_email_domain,
            commit_email::varchar as commit_email,
            iff(
                split_part(commit_email, '@', 2) = '',
                null,
                split_part(commit_email, '@', 2)
            ) as commit_email_domain,
            email_opted_in::boolean as is_email_opted_in,
            email_opted_in_source_id::number as email_opted_in_source_id,
            email_opted_in_at::timestamp as email_opted_in_at,
            dashboard::number as dashboard,
            project_view::number as project_view,
            consumed_timestep::number as consumed_timestep,
            layout::number as layout,
            hide_project_limit::boolean as has_hide_project_limit_enabled,
            -- note // hidden for privacy
            otp_grace_period_started_at::timestamp as otp_grace_period_started_at,
            external::boolean as is_external_user,
            organization
            as organization,  -- noqa:L029
            auditor::boolean as auditor,
            require_two_factor_authentication_from_group::boolean
            as does_require_two_factor_authentication_from_group,  -- noqa:L016
            two_factor_grace_period::number as two_factor_grace_period,
            last_activity_on::timestamp as last_activity_on,
            notified_of_own_activity::boolean as is_notified_of_own_activity,
            nullif(preferred_language, 'nan')::varchar as preferred_language,
            theme_id::number as theme_id,
            accepted_term_id::number as accepted_term_id,
            private_profile::boolean as is_private_profile,
            roadmap_layout::number as roadmap_layout,
            include_private_contributions::boolean as include_private_contributions,
            group_view::number as group_view,
            managing_group_id::number as managing_group_id,
            -- bot_type::NUMBER  // removed from prod
            role::number as role_id,
            {{ user_role_mapping(user_role="role") }}::varchar
            as role,  -- noqa:L029
            username::varchar as user_name,
            first_name::varchar as first_name,
            last_name::varchar as last_name,
            name::varchar as users_name,
            user_type::number as user_type

        from source

    ),

    add_job_hierarchy as (

        select renamed.*, {{ it_job_title_hierarchy("role") }} from renamed

    )

select *
from add_job_hierarchy
