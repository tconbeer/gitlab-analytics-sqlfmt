{{
    simple_cte(
        [
            ("source", "gitlab_dotcom_ci_runner_projects_source"),
            ("dim_date", "dim_date"),
        ]
    )
}},
renamed as (

    select
        ci_runner_project_id as dim_ci_runner_project_id,
        runner_id as dim_ci_runner_id,
        project_id as dim_project_id,
        date_id as dim_date_id,
        created_at,
        updated_at
    from source
    left join dim_date on to_date(created_at) = dim_date.date_day
)

{{
    dbt_audit(
        cte_ref="renamed",
        created_by="@mpeychet_",
        updated_by="@mpeychet_",
        created_date="2021-05-31",
        updated_date="2021-05-31",
    )
}}
