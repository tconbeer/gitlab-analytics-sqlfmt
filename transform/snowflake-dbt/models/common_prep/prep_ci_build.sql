{{ config(tags=["product"]) }}

{{ config({"materialized": "incremental", "unique_key": "dim_ci_build_id"}) }}

{{
    simple_cte(
        [
            ("dim_date", "dim_date"),
            ("dim_namespace_plan_hist", "dim_namespace_plan_hist"),
            ("plans", "gitlab_dotcom_plans_source"),
            ("prep_project", "prep_project"),
            ("prep_user", "prep_user"),
        ]
    )
}},
gitlab_dotcom_ci_builds_source as (

    select *
    from {{ ref("gitlab_dotcom_ci_builds_source") }}
    {% if is_incremental() %}

        where updated_at >= (select max(updated_at) from {{ this }})

    {% endif %}

),
renamed as (

    select
        ci_build_id as dim_ci_build_id,

        -- FOREIGN KEYS
        gitlab_dotcom_ci_builds_source.ci_build_project_id as dim_project_id,
        prep_project.dim_namespace_id,
        prep_project.ultimate_parent_namespace_id,
        dim_date.date_id as created_date_id,
        ifnull(dim_namespace_plan_hist.dim_plan_id, 34) as dim_plan_id,
        ci_build_runner_id as dim_ci_runner_id,
        ci_build_user_id as dim_user_id,
        ci_build_stage_id as dim_ci_stage_id,

        prep_project.namespace_is_internal,
        gitlab_dotcom_ci_builds_source.status as ci_build_status,
        gitlab_dotcom_ci_builds_source.finished_at,
        gitlab_dotcom_ci_builds_source.trace,
        gitlab_dotcom_ci_builds_source.created_at,
        gitlab_dotcom_ci_builds_source.updated_at,
        gitlab_dotcom_ci_builds_source.started_at,
        gitlab_dotcom_ci_builds_source.coverage,
        gitlab_dotcom_ci_builds_source.ci_build_commit_id as commit_id,
        gitlab_dotcom_ci_builds_source.ci_build_name,
        gitlab_dotcom_ci_builds_source.options,
        gitlab_dotcom_ci_builds_source.allow_failure,
        gitlab_dotcom_ci_builds_source.stage,
        gitlab_dotcom_ci_builds_source.ci_build_trigger_request_id
        as trigger_request_id,
        gitlab_dotcom_ci_builds_source.stage_idx,
        gitlab_dotcom_ci_builds_source.tag,
        gitlab_dotcom_ci_builds_source.ref,
        gitlab_dotcom_ci_builds_source.type as ci_build_type,
        gitlab_dotcom_ci_builds_source.target_url,
        gitlab_dotcom_ci_builds_source.description as ci_build_description,
        gitlab_dotcom_ci_builds_source.ci_build_erased_by_id as erased_by_id,
        gitlab_dotcom_ci_builds_source.ci_build_erased_at as erased_at,
        gitlab_dotcom_ci_builds_source.ci_build_artifacts_expire_at
        as artifacts_expire_at,
        gitlab_dotcom_ci_builds_source.environment,
        gitlab_dotcom_ci_builds_source.yaml_variables,
        gitlab_dotcom_ci_builds_source.ci_build_queued_at as queued_at,
        gitlab_dotcom_ci_builds_source.lock_version,
        gitlab_dotcom_ci_builds_source.coverage_regex,
        gitlab_dotcom_ci_builds_source.ci_build_auto_canceled_by_id
        as auto_canceled_by_id,
        gitlab_dotcom_ci_builds_source.retried,
        gitlab_dotcom_ci_builds_source.protected,
        gitlab_dotcom_ci_builds_source.failure_reason,
        gitlab_dotcom_ci_builds_source.ci_build_scheduled_at as scheduled_at,
        gitlab_dotcom_ci_builds_source.upstream_pipeline_id,
        case
            when ci_build_name like '%apifuzzer_fuzz%'
            then 'api_fuzzing'
            when ci_build_name like '%container_scanning%'
            then 'container_scanning'
            when ci_build_name like '%dast%'
            then 'dast'
            when ci_build_name like '%dependency_scanning%'
            then 'dependency_scanning'
            when ci_build_name like '%license_management%'
            then 'license_management'
            when ci_build_name like '%license_scanning%'
            then 'license_scanning'
            when ci_build_name like '%sast%'
            then 'sast'
            when ci_build_name like '%secret_detection%'
            then 'secret_detection'
        end as secure_ci_build_type

    from gitlab_dotcom_ci_builds_source
    left join
        prep_project
        on gitlab_dotcom_ci_builds_source.ci_build_project_id
        = prep_project.dim_project_id
    left join
        dim_namespace_plan_hist
        on prep_project.ultimate_parent_namespace_id
        = dim_namespace_plan_hist.dim_namespace_id
        and gitlab_dotcom_ci_builds_source.created_at
        >= dim_namespace_plan_hist.valid_from
        and gitlab_dotcom_ci_builds_source.created_at
        < coalesce(dim_namespace_plan_hist.valid_to, '2099-01-01')
    left join
        prep_user
        on gitlab_dotcom_ci_builds_source.ci_build_user_id = prep_user.dim_user_id
    left join
        dim_date
        on to_date(gitlab_dotcom_ci_builds_source.created_at) = dim_date.date_day
    where gitlab_dotcom_ci_builds_source.ci_build_project_id is not null

)

{{
    dbt_audit(
        cte_ref="renamed",
        created_by="@mpeychet_",
        updated_by="@ischweickartDD",
        created_date="2021-06-17",
        updated_date="2021-07-09",
    )
}}
