{{ config(tags=["product"]) }}

{{
    simple_cte(
        [
            ("dim_namespace_plan_hist", "dim_namespace_plan_hist"),
            ("plans", "gitlab_dotcom_plans_source"),
            ("prep_project", "prep_project"),
            ("prep_user", "prep_user"),
            ("gitlab_dotcom_ci_pipelines_source", "gitlab_dotcom_ci_pipelines_source"),
            ("dim_date", "dim_date"),
        ]
    )
}}

,
renamed as (

    select
        ci_pipeline_id as dim_ci_pipeline_id,

        -- FOREIGN KEYS
        gitlab_dotcom_ci_pipelines_source.project_id as dim_project_id,
        prep_project.dim_namespace_id,
        prep_project.ultimate_parent_namespace_id,
        prep_user.dim_user_id,
        dim_date.date_id as created_date_id,
        ifnull(dim_namespace_plan_hist.dim_plan_id, 34) as dim_plan_id,
        merge_request_id,

        gitlab_dotcom_ci_pipelines_source.created_at,
        gitlab_dotcom_ci_pipelines_source.started_at,
        gitlab_dotcom_ci_pipelines_source.committed_at,
        gitlab_dotcom_ci_pipelines_source.finished_at,
        gitlab_dotcom_ci_pipelines_source.ci_pipeline_duration
        as ci_pipeline_duration_in_s,

        gitlab_dotcom_ci_pipelines_source.status,
        gitlab_dotcom_ci_pipelines_source.ref,
        gitlab_dotcom_ci_pipelines_source.has_tag,
        gitlab_dotcom_ci_pipelines_source.yaml_errors,
        gitlab_dotcom_ci_pipelines_source.lock_version,
        gitlab_dotcom_ci_pipelines_source.auto_canceled_by_id,
        gitlab_dotcom_ci_pipelines_source.pipeline_schedule_id,
        gitlab_dotcom_ci_pipelines_source.ci_pipeline_source,
        gitlab_dotcom_ci_pipelines_source.config_source,
        gitlab_dotcom_ci_pipelines_source.is_protected,
        gitlab_dotcom_ci_pipelines_source.failure_reason as failure_reason_id,
        {{ map_ci_pipeline_failure_reason("failure_reason_id") }} as failure_reason,
        gitlab_dotcom_ci_pipelines_source.ci_pipeline_iid as ci_pipeline_internal_id
    from gitlab_dotcom_ci_pipelines_source
    left join
        prep_project
        on gitlab_dotcom_ci_pipelines_source.project_id = prep_project.dim_project_id
    left join
        dim_namespace_plan_hist
        on prep_project.ultimate_parent_namespace_id
        = dim_namespace_plan_hist.dim_namespace_id
        and gitlab_dotcom_ci_pipelines_source.created_at
        >= dim_namespace_plan_hist.valid_from
        and gitlab_dotcom_ci_pipelines_source.created_at < coalesce(
            dim_namespace_plan_hist.valid_to, '2099-01-01'
        )
    left join
        prep_user on gitlab_dotcom_ci_pipelines_source.user_id = prep_user.dim_user_id
    left join
        dim_date on to_date(
            gitlab_dotcom_ci_pipelines_source.created_at
        ) = dim_date.date_day
    where gitlab_dotcom_ci_pipelines_source.project_id is not null

)

{{
    dbt_audit(
        cte_ref="renamed",
        created_by="@mpeychet_",
        updated_by="@mpeychet_",
        created_date="2021-06-10",
        updated_date="2021-06-10",
    )
}}
