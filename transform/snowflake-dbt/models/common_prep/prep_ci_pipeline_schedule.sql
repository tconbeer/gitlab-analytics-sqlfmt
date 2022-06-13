{{ config(tags=["product"]) }}

{{
    config(
        {"materialized": "incremental", "unique_key": "dim_ci_pipeline_schedule_id"}
    )
}}

{{
    simple_cte(
        [
            ("dim_date", "dim_date"),
            ("dim_namespace_plan_hist", "dim_namespace_plan_hist"),
            ("dim_project", "dim_project"),
        ]
    )
}}

,
pipeline_schedule as (

    select *
    from {{ ref("gitlab_dotcom_ci_pipeline_schedules_source") }}
    {% if is_incremental() %}

    where updated_at >= (select max(updated_at) from {{ this }})

    {% endif %}

),
joined as (

    select
        ci_pipeline_schedule_id as dim_ci_pipeline_schedule_id,
        pipeline_schedule.owner_id as dim_user_id,
        pipeline_schedule.created_at,
        pipeline_schedule.updated_at,
        dim_date.date_id as created_date_id,
        ifnull(dim_project.dim_project_id, -1) as dim_project_id,
        ifnull(
            dim_project.ultimate_parent_namespace_id, -1
        ) as ultimate_parent_namespace_id,
        ifnull(dim_namespace_plan_hist.dim_plan_id, 34) as dim_plan_id
    from pipeline_schedule
    left join dim_project on pipeline_schedule.project_id = dim_project.dim_project_id
    left join
        dim_namespace_plan_hist
        on dim_project.ultimate_parent_namespace_id
        = dim_namespace_plan_hist.dim_namespace_id
        and pipeline_schedule.created_at >= dim_namespace_plan_hist.valid_from
        and pipeline_schedule.created_at < coalesce(
            dim_namespace_plan_hist.valid_to, '2099-01-01'
        )
    left join dim_date on to_date(pipeline_schedule.created_at) = dim_date.date_day

)

{{
    dbt_audit(
        cte_ref="joined",
        created_by="@chrissharp",
        updated_by="@chrissharp",
        created_date="2022-04-01",
        updated_date="2022-04-01",
    )
}}
