{{ config(tags=["product"]) }}

{{ config({"materialized": "incremental", "unique_key": "dim_ci_trigger_id"}) }}

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
ci_trigger_source as (

    select *
    from {{ ref("gitlab_dotcom_ci_triggers_source") }}
    {% if is_incremental() %}

    where updated_at >= (select max(updated_at) from {{ this }})

    {% endif %}

),
joined as (

    select
        ci_trigger_source.ci_trigger_id as dim_ci_trigger_id,
        ci_trigger_source.owner_id as owner_id,
        ifnull(dim_project.dim_project_id, -1) as dim_project_id,
        ifnull(
            dim_namespace_plan_hist.dim_namespace_id, -1
        ) as ultimate_parent_namespace_id,
        ifnull(dim_namespace_plan_hist.dim_plan_id, 34) as dim_plan_id,
        dim_date.date_id as created_date_id,
        ci_trigger_source.created_at as created_at,
        ci_trigger_source.updated_at as updated_at
    from ci_trigger_source
    left join dim_project on ci_trigger_source.project_id = dim_project.dim_project_id
    left join
        dim_namespace_plan_hist
        on dim_project.ultimate_parent_namespace_id
        = dim_namespace_plan_hist.dim_namespace_id
        and ci_trigger_source.created_at >= dim_namespace_plan_hist.valid_from
        and ci_trigger_source.created_at < coalesce(
            dim_namespace_plan_hist.valid_to, '2099-01-01'
        )
    left join dim_date on to_date(ci_trigger_source.created_at) = dim_date.date_day

)

{{
    dbt_audit(
        cte_ref="joined",
        created_by="@chrissharp",
        updated_by="@chrissharp",
        created_date="2022-04-06",
        updated_date="2022-04-06",
    )
}}
