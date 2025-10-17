with
    prep_ci_runner as (

        select
            dim_ci_runner_id,

            -- FOREIGN KEYS
            created_date_id,

            created_at,
            updated_at,
            ci_runner_description,
            case
                when ci_runner_description like '%private%manager%'
                then 'private-runner-mgr'
                when ci_runner_description like 'shared-runners-manager%'
                then 'linux-runner-mgr'
                when ci_runner_description like '%.shared.runners-manager.%'
                then 'linux-runner-mgr'
                when ci_runner_description like 'gitlab-shared-runners-manager%'
                then 'gitlab-internal-runner-mgr'
                when ci_runner_description like 'windows-shared-runners-manager%'
                then 'windows-runner-mgr'
                when ci_runner_description like '%.shared-gitlab-org.runners-manager.%'
                then 'shared-gitlab-org-runner-mgr'
                else 'Other'
            end as ci_runner_manager,
            contacted_at,
            is_active,
            ci_runner_version,
            revision,
            platform,
            architecture,
            is_untagged,
            is_locked,
            access_level,
            maximum_timeout,
            runner_type as ci_runner_type,
            case
                runner_type
                when 1
                then 'shared'
                when 2
                then 'group-runner-hosted runners'
                when 3
                then 'project-runner-hosted runners'
            end as ci_runner_type_summary,
            public_projects_minutes_cost_factor,
            private_projects_minutes_cost_factor

        from {{ ref("prep_ci_runner") }}

    )

    {{
        dbt_audit(
            cte_ref="prep_ci_runner",
            created_by="@snalamaru",
            updated_by="@davis_townsend",
            created_date="2021-06-23",
            updated_date="2021-11-09",
        )
    }}
