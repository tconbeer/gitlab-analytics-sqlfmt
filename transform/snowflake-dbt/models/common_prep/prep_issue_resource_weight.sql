{{ config(tags=["product"]) }}

{{ config({"materialized": "incremental", "unique_key": "dim_resource_weight_id"}) }}

{{
    simple_cte(
        [
            ("dim_date", "dim_date"),
            ("dim_namespace_plan_hist", "dim_namespace_plan_hist"),
            ("dim_project", "dim_project"),
            ("dim_issue", "dim_issue"),
        ]
    )
}},
resource_weight_events as (

    select *
    from {{ ref("gitlab_dotcom_resource_weight_events_source") }}
    {% if is_incremental() %}

    where created_at >= (select max(created_at) from {{ this }}) {% endif %}

),
joined as (

    select
        resource_weight_event_id as dim_resource_weight_id,
        resource_weight_events.user_id as dim_user_id,
        resource_weight_events.created_at,
        dim_date.date_id as created_date_id,
        ifnull(dim_project.dim_project_id, -1) as dim_project_id,
        ifnull(
            dim_project.ultimate_parent_namespace_id, -1
        ) as ultimate_parent_namespace_id,
        ifnull(dim_namespace_plan_hist.dim_plan_id, 34) as dim_plan_id
    from resource_weight_events
    left join dim_issue on resource_weight_events.issue_id = dim_issue.dim_issue_id
    left join dim_project on dim_issue.dim_project_id = dim_project.dim_project_id
    left join
        dim_namespace_plan_hist
        on dim_project.ultimate_parent_namespace_id
        = dim_namespace_plan_hist.dim_namespace_id
        and resource_weight_events.created_at >= dim_namespace_plan_hist.valid_from
        and resource_weight_events.created_at
        < coalesce(dim_namespace_plan_hist.valid_to, '2099-01-01')
    left join dim_date on to_date(resource_weight_events.created_at) = dim_date.date_day

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
