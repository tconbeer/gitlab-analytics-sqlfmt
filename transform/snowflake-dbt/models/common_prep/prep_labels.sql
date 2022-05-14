{{ config(tags=["product"]) }}

{{
    simple_cte(
        [
            ("gitlab_dotcom_labels_source", "gitlab_dotcom_labels_source"),
            ("dim_date", "dim_date"),
            ("dim_namespace_plan_hist", "dim_namespace_plan_hist"),
            ("dim_project", "dim_project"),
        ]
    )
}}

,
renamed as (

    select
        gitlab_dotcom_labels_source.label_id as dim_label_id,
        -- FOREIGN KEYS
        gitlab_dotcom_labels_source.project_id as dim_project_id,
        ifnull(
            gitlab_dotcom_labels_source.group_id,
            dim_project.ultimate_parent_namespace_id
        ) as ultimate_parent_namespace_id,
        ifnull(dim_namespace_plan_hist.dim_plan_id, 34) as dim_plan_id,
        -- 
        gitlab_dotcom_labels_source.group_id as dim_namespace_id,
        gitlab_dotcom_labels_source.label_title,
        gitlab_dotcom_labels_source.label_type,
        gitlab_dotcom_labels_source.created_at,
        dim_date.date_id as created_date_id
    from gitlab_dotcom_labels_source
    left join
        dim_project
        on gitlab_dotcom_labels_source.project_id = dim_project.dim_project_id
    left join
        dim_namespace_plan_hist
        on dim_project.ultimate_parent_namespace_id
        = dim_namespace_plan_hist.dim_namespace_id
        and gitlab_dotcom_labels_source.created_at >= dim_namespace_plan_hist.valid_from
        and gitlab_dotcom_labels_source.created_at < coalesce(
            dim_namespace_plan_hist.valid_to, '2099-01-01'
        )
    left join
        dim_date on to_date(gitlab_dotcom_labels_source.created_at) = dim_date.date_day

)

{{
    dbt_audit(
        cte_ref="renamed",
        created_by="@dtownsend",
        updated_by="@chrissharp",
        created_date="2021-08-04",
        updated_date="2022-03-23",
    )
}}
