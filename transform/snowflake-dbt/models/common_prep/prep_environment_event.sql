{{ config(tags=["product"]) }}

{{ config({"materialized": "incremental", "unique_key": "dim_environment_id"}) }}

{{
    simple_cte(
        [
            ("dim_date", "dim_date"),
            ("dim_namespace_plan_hist", "dim_namespace_plan_hist"),
            ("dim_project", "dim_project"),
        ]
    )
}},
environment_event as (

    select *
    from {{ ref("gitlab_dotcom_environments_source") }}
    {% if is_incremental() %}

    where updated_at >= (select max(updated_at) from {{ this }})

    {% endif %}

),
joined as (

    select
        environment_event.environment_id as dim_environment_id,
        ifnull(environment_event.project_id, -1) as dim_project_id,
        ifnull(
            dim_project.ultimate_parent_namespace_id, -1
        ) as ultimate_parent_namespace_id,
        ifnull(dim_namespace_plan_hist.dim_plan_id, 34) as dim_plan_id,
        environment_event.created_at::timestamp as created_at,
        environment_event.updated_at::timestamp as updated_at,
        dim_date.date_id as created_date_id
    from environment_event
    left join dim_project on environment_event.project_id = dim_project.dim_project_id
    left join
        dim_namespace_plan_hist
        on dim_project.ultimate_parent_namespace_id
        = dim_namespace_plan_hist.dim_namespace_id
        and environment_event.created_at >= dim_namespace_plan_hist.valid_from
        and environment_event.created_at
        < coalesce(dim_namespace_plan_hist.valid_to, '2099-01-01')
    left join dim_date on to_date(environment_event.created_at) = dim_date.date_day

)

{{
    dbt_audit(
        cte_ref="joined",
        created_by="@chrissharp",
        updated_by="@chrissharp",
        created_date="2022-03-16",
        updated_date="2022-03-16",
    )
}}
