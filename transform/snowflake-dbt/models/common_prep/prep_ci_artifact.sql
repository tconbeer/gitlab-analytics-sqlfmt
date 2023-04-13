{{ config(tags=["product"]) }}

{{ config({"materialized": "incremental", "unique_key": "dim_ci_job_artifact_id"}) }}

{{
    simple_cte(
        [
            ("dim_date", "dim_date"),
            ("dim_namespace_plan_hist", "dim_namespace_plan_hist"),
            ("dim_project", "dim_project"),
        ]
    )
}},
ci_job_artifacts as (

    select *
    from {{ ref("gitlab_dotcom_ci_job_artifacts_source") }}
    {% if is_incremental() %}

        where updated_at >= (select max(updated_at) from {{ this }})

    {% endif %}

),
joined as (

    select
        ci_job_artifact_id as dim_ci_job_artifact_id,
        project_id as dim_project_id,
        ifnull(
            dim_project.ultimate_parent_namespace_id, -1
        ) as ultimate_parent_namespace_id,
        ifnull(dim_namespace_plan_hist.dim_plan_id, 34) as dim_plan_id,
        file_type,
        ci_job_artifacts.created_at,
        ci_job_artifacts.updated_at,
        dim_date.date_id as created_date_id
    from ci_job_artifacts
    left join dim_project on ci_job_artifacts.project_id = dim_project.dim_project_id
    left join
        dim_namespace_plan_hist
        on dim_project.ultimate_parent_namespace_id
        = dim_namespace_plan_hist.dim_namespace_id
        and ci_job_artifacts.created_at >= dim_namespace_plan_hist.valid_from
        and ci_job_artifacts.created_at
        < coalesce(dim_namespace_plan_hist.valid_to, '2099-01-01')
    left join dim_date on to_date(ci_job_artifacts.created_at) = dim_date.date_day

)

{{
    dbt_audit(
        cte_ref="joined",
        created_by="@chrissharp",
        updated_by="@chrissharp",
        created_date="2022-03-24",
        updated_date="2022-03-24",
    )
}}
