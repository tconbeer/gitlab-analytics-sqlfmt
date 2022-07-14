{{ config(tags=["product"]) }}

{{ config({"materialized": "incremental", "unique_key": "dim_milestone_id"}) }}

{{
    simple_cte(
        [
            ("dim_date", "dim_date"),
            ("dim_namespace_plan_hist", "dim_namespace_plan_hist"),
            ("dim_project", "dim_project"),
            ("dim_issue", "dim_issue"),
            ("dim_epic", "dim_epic"),
        ]
    )
}},
milestones as (

    select *
    from {{ ref("gitlab_dotcom_milestones_source") }}
    {% if is_incremental() %}

    where updated_at >= (select max(updated_at) from {{ this }}) {% endif %}

),
joined as (

    select
        milestone_id as dim_milestone_id,
        milestones.created_at,
        milestones.updated_at,
        dim_date.date_id as created_date_id,
        ifnull(dim_project.dim_project_id, -1) as dim_project_id,
        coalesce(
            dim_project.ultimate_parent_namespace_id, milestones.group_id, -1
        ) as ultimate_parent_namespace_id,
        coalesce(
            dim_namespace_plan_hist.dim_plan_id, dim_epic.dim_plan_id, 34
        ) as dim_plan_id
    from milestones
    left join dim_project on milestones.project_id = dim_project.dim_project_id
    left join dim_epic on milestones.group_id = dim_epic.dim_epic_id
    left join
        dim_namespace_plan_hist
        on dim_project.ultimate_parent_namespace_id
        = dim_namespace_plan_hist.dim_namespace_id
        and milestones.created_at >= dim_namespace_plan_hist.valid_from
        and milestones.created_at
        < coalesce(dim_namespace_plan_hist.valid_to, '2099-01-01')
    left join dim_date as dim_date on to_date(milestones.created_at) = dim_date.date_day

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
