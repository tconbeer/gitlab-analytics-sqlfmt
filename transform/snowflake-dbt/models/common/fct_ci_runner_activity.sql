{{ config(tags=["product"]) }}


{{
    simple_cte(
        [
            ("prep_ci_build", "prep_ci_build"),
            ("prep_ci_runner", "prep_ci_runner"),
            ("prep_ci_pipeline", "prep_ci_pipeline"),
            ("prep_ci_stage", "prep_ci_stage"),
            ("prep_project", "prep_project"),
            ("dim_namespace", "dim_namespace"),
            ("prep_user", "prep_user"),
            ("dim_date", "dim_date"),
        ]
    )
}},
joined as (

    select
        -- PRIMARY KEY
        prep_ci_build.dim_ci_build_id,

        -- FOREIGN KEYS
        ifnull(prep_ci_runner.dim_ci_runner_id, -1) as dim_ci_runner_id,
        ifnull(prep_ci_pipeline.dim_ci_pipeline_id, -1) as dim_ci_pipeline_id,
        ifnull(prep_ci_stage.dim_ci_stage_id, -1) as dim_ci_stage_id,
        ifnull(prep_project.dim_project_id, -1) as dim_project_id,
        ifnull(prep_user.dim_user_id, -1) as dim_user_id,
        ifnull(dim_date.date_id, -1) as ci_build_created_date_id,
        ifnull(dim_namespace.dim_namespace_id, -1) as dim_namespace_id,
        ifnull(
            dim_namespace.ultimate_parent_namespace_id, -1
        ) as ultimate_parent_namespace_id,
        prep_ci_build.dim_plan_id,

        -- ci_build metrics
        prep_ci_build.started_at as ci_build_started_at,
        prep_ci_build.finished_at as ci_build_finished_at,
        datediff(
            'seconds', prep_ci_build.started_at, prep_ci_build.finished_at
        ) as ci_build_duration_in_s,

        -- ci_runner metrics
        case
            when dim_namespace.namespace_is_internal = true
            then true
            when prep_ci_runner.runner_type = 1
            then true
            else false
        end as is_paid_by_gitlab,
        prep_ci_runner.public_projects_minutes_cost_factor,
        prep_ci_runner.private_projects_minutes_cost_factor

    from prep_ci_build
    left join
        prep_ci_runner
        on prep_ci_build.dim_ci_runner_id = prep_ci_runner.dim_ci_runner_id
    left join
        prep_ci_stage on prep_ci_build.dim_ci_stage_id = prep_ci_stage.dim_ci_stage_id
    left join
        prep_ci_pipeline
        on prep_ci_stage.dim_ci_pipeline_id = prep_ci_pipeline.dim_ci_pipeline_id
    left join prep_project on prep_ci_build.dim_project_id = prep_project.dim_project_id
    left join
        dim_namespace on prep_ci_build.dim_namespace_id = dim_namespace.dim_namespace_id
    left join prep_user on prep_ci_build.dim_user_id = prep_user.dim_user_id
    left join dim_date on prep_ci_build.created_date_id = dim_date.date_id

)

{{
    dbt_audit(
        cte_ref="joined",
        created_by="@mpeychet_",
        updated_by="@ischweickartDD",
        created_date="2021-06-30",
        updated_date="2021-07-14",
    )
}}
