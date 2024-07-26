{{ config(tags=["product"]) }}

{{ config({"materialized": "incremental", "unique_key": "dim_issue_label_id"}) }}

{{
    simple_cte(
        [
            ("dim_date", "dim_date"),
            ("dim_epic", "dim_epic"),
            ("dim_issue", "dim_issue"),
            ("dim_merge_request", "dim_merge_request"),
        ]
    )
}},
resource_label_events as (

    select *
    from {{ ref("gitlab_dotcom_resource_label_events_source") }}
    {% if is_incremental() %}

        where created_at >= (select max(created_at) from {{ this }})

    {% endif %}

),
joined as (

    select
        resource_label_events.resource_label_event_id as dim_issue_label_id,
        coalesce(
            dim_issue.dim_project_id, dim_merge_request.dim_project_id
        ) as dim_project_id,
        coalesce(
            dim_epic.dim_plan_id, dim_issue.dim_plan_id, dim_merge_request.dim_plan_id
        ) as dim_plan_id,
        coalesce(
            dim_epic.group_id,
            dim_issue.ultimate_parent_namespace_id,
            dim_merge_request.ultimate_parent_namespace_id
        ) as ultimate_parent_namespace_id,
        user_id as dim_user_id,
        dim_issue.dim_issue_id as dim_issue_id,
        dim_merge_request.dim_merge_request_id as dim_merge_request_id,
        dim_epic.dim_epic_id as dim_epic_id,
        resource_label_events.created_at::timestamp as created_at,
        dim_date.date_id as created_date_id
    from resource_label_events
    left join dim_epic on resource_label_events.epic_id = dim_epic.dim_epic_id
    left join dim_issue on resource_label_events.issue_id = dim_issue.dim_issue_id
    left join
        dim_merge_request
        on resource_label_events.merge_request_id
        = dim_merge_request.dim_merge_request_id
    left join dim_date on to_date(resource_label_events.created_at) = dim_date.date_day

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
