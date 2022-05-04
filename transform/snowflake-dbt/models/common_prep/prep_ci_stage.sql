{{ config(tags=["product"]) }}

{{ config({"materialized": "incremental", "unique_key": "dim_ci_stage_id"}) }}

{{
    simple_cte(
        [
            ("dim_project", "dim_project"),
            ("dim_ci_pipeline", "dim_ci_pipeline"),
            ("dim_namespace_plan_hist", "dim_namespace_plan_hist"),
            ("dim_date", "dim_date"),
        ]
    )
}}

,
ci_stages as (

    select *
    from {{ ref("gitlab_dotcom_ci_stages_dedupe_source") }}
    where created_at is not null

),
joined as (

    select
        ci_stages.id as dim_ci_stage_id,
        ifnull(dim_project.dim_project_id, -1) as dim_project_id,
        ifnull(dim_ci_pipeline.dim_ci_pipeline_id, -1) as dim_ci_pipeline_id,
        ifnull(dim_namespace_plan_hist.dim_plan_id, 34) as dim_plan_id,
        ifnull(
            dim_namespace_plan_hist.dim_namespace_id, -1
        ) as ultimate_parent_namespace_id,
        dim_date.date_id as created_date_id,
        ci_stages.created_at::timestamp as created_at,
        ci_stages.updated_at::timestamp as updated_at,
        ci_stages.name as ci_stage_name,
        ci_stages.status as ci_stage_status,
        ci_stages.lock_version as lock_version,
        ci_stages.position as position
    from ci_stages
    left join dim_project on ci_stages.project_id = dim_project.dim_project_id
    left join
        dim_namespace_plan_hist
        on dim_project.ultimate_parent_namespace_id
        = dim_namespace_plan_hist.dim_namespace_id
        and ci_stages.created_at >= dim_namespace_plan_hist.valid_from
        and ci_stages.created_at < coalesce(
            dim_namespace_plan_hist.valid_to, '2099-01-01'
        )
    left join
        dim_ci_pipeline on ci_stages.pipeline_id = dim_ci_pipeline.dim_ci_pipeline_id
    left join dim_date on to_date(ci_stages.created_at) = dim_date.date_day

)

{{
    dbt_audit(
        cte_ref="joined",
        created_by="@mpeychet_",
        updated_by="@chrissharp",
        created_date="2021-06-29",
        updated_date="2022-03-14",
    )
}}
