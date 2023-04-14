{{ config(tags=["product"]) }}

{{ config({"materialized": "incremental", "unique_key": "dim_deployment_id"}) }}

{{
    simple_cte(
        [
            ("dim_date", "dim_date"),
            ("prep_namespace_plan_hist", "prep_namespace_plan_hist"),
            ("plans", "gitlab_dotcom_plans_source"),
            ("prep_namespace", "prep_namespace"),
            ("prep_project", "prep_project"),
        ]
    )
}},
gitlab_dotcom_deployments_dedupe_source as (

    select *
    from {{ ref("gitlab_dotcom_deployments_dedupe_source") }}
    {% if is_incremental() %}

        where updated_at >= (select max(updated_at) from {{ this }})

    {% endif %}

),
prep_user as (

    select *
    from {{ ref("prep_user") }} users
    where {{ filter_out_blocked_users("users", "dim_user_id") }}

),
joined as (

    select
        gitlab_dotcom_deployments_dedupe_source.id::number as dim_deployment_id,
        gitlab_dotcom_deployments_dedupe_source.project_id::number as dim_project_id,
        prep_project.ultimate_parent_namespace_id::number
        as ultimate_parent_namespace_id,
        dim_date.date_id::number as created_date_id,
        ifnull(prep_namespace_plan_hist.dim_plan_id, 34)::number as dim_plan_id,
        prep_user.dim_user_id::number as dim_user_id,
        gitlab_dotcom_deployments_dedupe_source.iid::number as deployment_internal_id,
        gitlab_dotcom_deployments_dedupe_source.environment_id::number
        as environment_id,
        gitlab_dotcom_deployments_dedupe_source.cluster_id::number as cluster_id,
        gitlab_dotcom_deployments_dedupe_source.deployable_id::number as deployable_id,
        gitlab_dotcom_deployments_dedupe_source.deployable_type::varchar
        as deployable_type,
        gitlab_dotcom_deployments_dedupe_source.created_at::timestamp as created_at,
        gitlab_dotcom_deployments_dedupe_source.updated_at::timestamp as updated_at,
        gitlab_dotcom_deployments_dedupe_source.on_stop::varchar as on_stop,
        gitlab_dotcom_deployments_dedupe_source.finished_at::timestamp as finished_at,
        gitlab_dotcom_deployments_dedupe_source.status::number as status_id
    from gitlab_dotcom_deployments_dedupe_source
    left join
        prep_project
        on gitlab_dotcom_deployments_dedupe_source.project_id
        = prep_project.dim_project_id
    left join
        prep_namespace
        on prep_project.ultimate_parent_namespace_id = prep_namespace.dim_namespace_id
        and prep_namespace.is_currently_valid = true
    left join
        prep_namespace_plan_hist
        on prep_project.ultimate_parent_namespace_id
        = prep_namespace_plan_hist.dim_namespace_id
        and gitlab_dotcom_deployments_dedupe_source.created_at
        >= prep_namespace_plan_hist.valid_from
        and gitlab_dotcom_deployments_dedupe_source.created_at
        < coalesce(prep_namespace_plan_hist.valid_to, '2099-01-01')
    left join
        prep_user
        on gitlab_dotcom_deployments_dedupe_source.user_id = prep_user.dim_user_id
    left join
        dim_date
        on to_date(gitlab_dotcom_deployments_dedupe_source.created_at)
        = dim_date.date_day

)

{{
    dbt_audit(
        cte_ref="joined",
        created_by="@mpeychet_",
        updated_by="@chrissharp",
        created_date="2021-07-26",
        updated_date="2022-03-09",
    )
}}
