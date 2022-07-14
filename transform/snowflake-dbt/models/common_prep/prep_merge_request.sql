{{ config(tags=["product"]) }}

{{ config({"materialized": "incremental", "unique_key": "dim_merge_request_id"}) }}

{{
    simple_cte(
        [
            ("dim_date", "dim_date"),
            ("dim_namespace_plan_hist", "dim_namespace_plan_hist"),
            ("plans", "gitlab_dotcom_plans_source"),
            ("prep_project", "prep_project"),
            ("prep_user", "prep_user"),
        ]
    )
}},
gitlab_dotcom_merge_requests_source as (

    select *
    from {{ ref("gitlab_dotcom_merge_requests_source") }}
    {% if is_incremental() %}

    where updated_at >= (select max(updated_at) from {{ this }}) {% endif %}

),
renamed as (

    select
        gitlab_dotcom_merge_requests_source.merge_request_id as dim_merge_request_id,

        -- FOREIGN KEYS
        gitlab_dotcom_merge_requests_source.target_project_id as dim_project_id,
        prep_project.dim_namespace_id,
        prep_project.ultimate_parent_namespace_id,
        dim_date.date_id as created_date_id,
        ifnull(dim_namespace_plan_hist.dim_plan_id, 34) as dim_plan_id,
        gitlab_dotcom_merge_requests_source.author_id,
        gitlab_dotcom_merge_requests_source.milestone_id,
        gitlab_dotcom_merge_requests_source.assignee_id,
        gitlab_dotcom_merge_requests_source.merge_user_id,
        gitlab_dotcom_merge_requests_source.updated_by_id,
        gitlab_dotcom_merge_requests_source.last_edited_by_id,
        gitlab_dotcom_merge_requests_source.head_pipeline_id as head_ci_pipeline_id,

        gitlab_dotcom_merge_requests_source.merge_request_iid
        as merge_request_internal_id,
        gitlab_dotcom_merge_requests_source.merge_request_title,
        gitlab_dotcom_merge_requests_source.is_merge_to_master,
        gitlab_dotcom_merge_requests_source.merge_error,
        gitlab_dotcom_merge_requests_source.latest_merge_request_diff_id,
        gitlab_dotcom_merge_requests_source.approvals_before_merge,
        gitlab_dotcom_merge_requests_source.lock_version,
        gitlab_dotcom_merge_requests_source.time_estimate,
        gitlab_dotcom_merge_requests_source.project_id,
        gitlab_dotcom_merge_requests_source.merge_request_state_id,
        gitlab_dotcom_merge_requests_source.merge_request_state,
        gitlab_dotcom_merge_requests_source.merge_request_status,
        gitlab_dotcom_merge_requests_source.does_merge_when_pipeline_succeeds,
        gitlab_dotcom_merge_requests_source.does_squash,
        gitlab_dotcom_merge_requests_source.is_discussion_locked,
        gitlab_dotcom_merge_requests_source.does_allow_maintainer_to_push,
        gitlab_dotcom_merge_requests_source.created_at,
        gitlab_dotcom_merge_requests_source.updated_at,
        gitlab_dotcom_merge_requests_source.merge_request_last_edited_at

    from gitlab_dotcom_merge_requests_source
    left join
        prep_project
        on gitlab_dotcom_merge_requests_source.target_project_id
        = prep_project.dim_project_id
    left join
        dim_namespace_plan_hist
        on prep_project.ultimate_parent_namespace_id
        = dim_namespace_plan_hist.dim_namespace_id
        and gitlab_dotcom_merge_requests_source.created_at
        >= dim_namespace_plan_hist.valid_from
        and gitlab_dotcom_merge_requests_source.created_at
        < coalesce(dim_namespace_plan_hist.valid_to, '2099-01-01')
    left join
        dim_date
        on to_date(gitlab_dotcom_merge_requests_source.created_at) = dim_date.date_day
    where gitlab_dotcom_merge_requests_source.project_id is not null

)

{{
    dbt_audit(
        cte_ref="renamed",
        created_by="@mpeychet_",
        updated_by="@mpeychet_",
        created_date="2021-06-17",
        updated_date="2021-06-17",
    )
}}
