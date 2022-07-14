{{ config(tags=["product"]) }}

{{ config({"materialized": "incremental", "unique_key": "dim_resource_milestone_id"}) }}

{{
    simple_cte(
        [
            ("dim_date", "dim_date"),
            ("dim_issue", "dim_issue"),
            ("dim_merge_request", "dim_merge_request"),
        ]
    )
}},
resource_milestone_events as (

    select *
    from {{ ref("gitlab_dotcom_resource_milestone_events_source") }}
    {% if is_incremental() %}

    where created_at >= (select max(created_at) from {{ this }})

    {% endif %}

),
joined as (

    select
        resource_milestone_events.resource_milestone_event_id
        as dim_resource_milestone_id,
        coalesce(
            dim_issue.dim_project_id, dim_merge_request.dim_project_id
        ) as dim_project_id,
        coalesce(dim_issue.dim_plan_id, dim_merge_request.dim_plan_id) as dim_plan_id,
        coalesce(
            dim_issue.ultimate_parent_namespace_id,
            dim_merge_request.ultimate_parent_namespace_id
        ) as ultimate_parent_namespace_id,
        user_id as dim_user_id,
        issue_id,
        merge_request_id,
        resource_milestone_events.created_at,
        dim_date.date_id as created_date_id
    from resource_milestone_events
    left join dim_issue on resource_milestone_events.issue_id = dim_issue.dim_issue_id
    left join
        dim_merge_request
        on resource_milestone_events.merge_request_id
        = dim_merge_request.dim_merge_request_id
    left join
        dim_date on to_date(resource_milestone_events.created_at) = dim_date.date_day

)

{{
    dbt_audit(
        cte_ref="joined",
        created_by="@chrissharp",
        updated_by="@chrissharp",
        created_date="2022-03-23",
        updated_date="2022-03-23",
    )
}}
