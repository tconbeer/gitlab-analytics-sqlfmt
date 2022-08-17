{{ config({"schema": "common_mart_product"}) }}

{{
    simple_cte(
        [
            ("ci_runner_activity", "fct_ci_runner_activity"),
            ("dim_namespace", "dim_namespace"),
            ("dim_date", "dim_date"),
        ]
    )
}},
ci_runner_activity_monthly as (

    select
        dim_date.first_day_of_month as report_month,
        ci_runner_activity.dim_namespace_id,
        ci_runner_activity.dim_ci_runner_id,
        ci_runner_activity.dim_ci_pipeline_id,
        ci_runner_activity.dim_ci_stage_id,
        ci_runner_activity.is_paid_by_gitlab,
        ci_runner_activity.public_projects_minutes_cost_factor,
        ci_runner_activity.private_projects_minutes_cost_factor,
        sum(ci_build_duration_in_s) as ci_build_duration_in_s
    from ci_runner_activity
    inner join
        dim_date on to_date(ci_runner_activity.ci_build_started_at) = dim_date.date_day
    where
        ci_runner_activity.ci_build_finished_at is not null
        {{ dbt_utils.group_by(n=8) }}

),
joined as (

    select
        ci_runner_activity_monthly.report_month,
        dim_namespace.dim_namespace_id,
        dim_namespace.ultimate_parent_namespace_id,
        dim_namespace.gitlab_plan_id as dim_plan_id,
        ci_runner_activity_monthly.dim_ci_runner_id,
        ci_runner_activity_monthly.dim_ci_pipeline_id,
        ci_runner_activity_monthly.dim_ci_stage_id,


        -- CI RUNNER METRICS
        ci_runner_activity_monthly.ci_build_duration_in_s,
        ci_runner_activity_monthly.public_projects_minutes_cost_factor,
        ci_runner_activity_monthly.private_projects_minutes_cost_factor,

        -- CI RUNNER ACTIVITY METADATA
        ci_runner_activity_monthly.is_paid_by_gitlab,
        dim_namespace.namespace_is_internal,
        dim_namespace.gitlab_plan_title as ultimate_parent_plan_title

    from ci_runner_activity_monthly
    inner join
        dim_namespace
        on ci_runner_activity_monthly.dim_namespace_id = dim_namespace.dim_namespace_id

)

{{
    dbt_audit(
        cte_ref="joined",
        created_by="@ischweickartDD",
        updated_by="@chrissharp",
        created_date="2021-07-30",
        updated_date="2021-10-11",
    )
}}
