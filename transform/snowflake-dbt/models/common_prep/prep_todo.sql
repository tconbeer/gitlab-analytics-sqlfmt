{{ config(tags=["product"]) }}

{{ config({"materialized": "incremental", "unique_key": "dim_todo_id"}) }}

{{
    simple_cte(
        [
            ("dim_date", "dim_date"),
            ("dim_namespace_plan_hist", "dim_namespace_plan_hist"),
            ("dim_project", "dim_project"),
        ]
    )
}},
gitlab_dotcom_todo_dedupe_source as (

    select *
    from {{ ref("gitlab_dotcom_todos_source") }}
    {% if is_incremental() %}

    where updated_at >= (select max(updated_at) from {{ this }})

    {% endif %}

),
joined as (

    select
        gitlab_dotcom_todo_dedupe_source.todo_id as dim_todo_id,
        gitlab_dotcom_todo_dedupe_source.author_id as author_id,
        ifnull(dim_project.dim_project_id, -1) as dim_project_id,
        ifnull(
            dim_namespace_plan_hist.dim_namespace_id, -1
        ) as ultimate_parent_namespace_id,
        ifnull(dim_namespace_plan_hist.dim_plan_id, 34) as dim_plan_id,
        dim_date.date_id as created_date_id,
        gitlab_dotcom_todo_dedupe_source.created_at::timestamp as created_at,
        gitlab_dotcom_todo_dedupe_source.updated_at::timestamp as updated_at
    from gitlab_dotcom_todo_dedupe_source
    left join
        dim_project
        on gitlab_dotcom_todo_dedupe_source.project_id = dim_project.dim_project_id
    left join
        dim_namespace_plan_hist
        on dim_project.ultimate_parent_namespace_id
        = dim_namespace_plan_hist.dim_namespace_id
        and gitlab_dotcom_todo_dedupe_source.created_at
        >= dim_namespace_plan_hist.valid_from
        and gitlab_dotcom_todo_dedupe_source.created_at < coalesce(
            dim_namespace_plan_hist.valid_to, '2099-01-01'
        )
    left join
        dim_date
        on to_date(gitlab_dotcom_todo_dedupe_source.created_at) = dim_date.date_day

)

{{
    dbt_audit(
        cte_ref="joined",
        created_by="@chrissharp",
        updated_by="@chrissharp",
        created_date="2022-03-14",
        updated_date="2022-03-14",
    )
}}
