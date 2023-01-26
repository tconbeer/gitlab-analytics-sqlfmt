{{ config(tags=["product"]) }}

{{ config({"materialized": "incremental", "unique_key": "dim_package_id"}) }}

{{
    simple_cte(
        [
            ("dim_date", "dim_date"),
            ("dim_namespace_plan_hist", "dim_namespace_plan_hist"),
            ("plans", "gitlab_dotcom_plans_source"),
            ("prep_namespace", "prep_namespace"),
            ("prep_project", "prep_project"),
            ("prep_user", "prep_user"),
        ]
    )
}},
gitlab_dotcom_packages_packages_dedupe_source as (

    select *
    from {{ ref("gitlab_dotcom_packages_packages_dedupe_source") }}
    {% if is_incremental() %}

    where updated_at >= (select max(updated_at) from {{ this }})

    {% endif %}

),
renamed as (

    select
        id::number as dim_package_id,

        -- FOREIGN KEYS
        prep_project.dim_project_id as dim_project_id,
        prep_namespace.dim_namespace_id,
        prep_namespace.ultimate_parent_namespace_id,
        dim_date.date_id as created_date_id,
        ifnull(dim_namespace_plan_hist.dim_plan_id, 34) as dim_plan_id,
        prep_user.dim_user_id as creator_id,

        prep_project.namespace_is_internal,

        version::varchar as package_version,
        package_type::varchar as package_type,
        gitlab_dotcom_packages_packages_dedupe_source.created_at::timestamp
        as created_at,
        gitlab_dotcom_packages_packages_dedupe_source.updated_at::timestamp
        as updated_at

    from gitlab_dotcom_packages_packages_dedupe_source
    left join
        prep_project
        on gitlab_dotcom_packages_packages_dedupe_source.project_id
        = prep_project.dim_project_id
    left join
        dim_namespace_plan_hist
        on prep_project.ultimate_parent_namespace_id
        = dim_namespace_plan_hist.dim_namespace_id
        and gitlab_dotcom_packages_packages_dedupe_source.created_at
        >= dim_namespace_plan_hist.valid_from
        and gitlab_dotcom_packages_packages_dedupe_source.created_at
        < coalesce(dim_namespace_plan_hist.valid_to, '2099-01-01')
    left join
        prep_namespace
        on prep_project.dim_namespace_id = prep_namespace.dim_namespace_id
        and is_currently_valid = true
    left join
        prep_user
        on gitlab_dotcom_packages_packages_dedupe_source.creator_id
        = prep_user.dim_user_id
    left join
        dim_date
        on to_date(gitlab_dotcom_packages_packages_dedupe_source.created_at)
        = dim_date.date_day

)

{{
    dbt_audit(
        cte_ref="renamed",
        created_by="@mpeychet_",
        updated_by="@mpeychet_",
        created_date="2021-08-05",
        updated_date="2021-08-05",
    )
}}
