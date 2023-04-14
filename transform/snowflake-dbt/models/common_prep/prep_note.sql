{{ config(tags=["product"]) }}

{{ config({"materialized": "incremental", "unique_key": "dim_note_id"}) }}

{{
    simple_cte(
        [
            ("dim_date", "dim_date"),
            ("dim_namespace_plan_hist", "dim_namespace_plan_hist"),
            ("plans", "gitlab_dotcom_plans_source"),
            ("prep_project", "prep_project"),
            ("dim_epic", "dim_epic"),
            ("dim_namespace", "dim_namespace"),
        ]
    )
}},
gitlab_dotcom_notes_dedupe_source as (

    select *
    from {{ ref("gitlab_dotcom_notes_dedupe_source") }}
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
        gitlab_dotcom_notes_dedupe_source.id::number as dim_note_id,
        gitlab_dotcom_notes_dedupe_source.author_id::number as author_id,
        gitlab_dotcom_notes_dedupe_source.project_id::number as dim_project_id,
        ifnull(
            prep_project.ultimate_parent_namespace_id::number,
            dim_namespace.ultimate_parent_namespace_id::number
        ) as ultimate_parent_namespace_id,
        gitlab_dotcom_notes_dedupe_source.noteable_id::number as noteable_id,
        dim_date.date_id::number as created_date_id,
        ifnull(dim_namespace_plan_hist.dim_plan_id, 34)::number as dim_plan_id,
        iff(noteable_type = '', null, noteable_type)::varchar as noteable_type,
        gitlab_dotcom_notes_dedupe_source.created_at::timestamp as created_at,
        gitlab_dotcom_notes_dedupe_source.updated_at::timestamp as updated_at,
        gitlab_dotcom_notes_dedupe_source.note::varchar as note,
        gitlab_dotcom_notes_dedupe_source.attachment::varchar as attachment,
        gitlab_dotcom_notes_dedupe_source.line_code::varchar as line_code,
        gitlab_dotcom_notes_dedupe_source.commit_id::varchar as commit_id,
        gitlab_dotcom_notes_dedupe_source.system::boolean as is_system_note,
        gitlab_dotcom_notes_dedupe_source.updated_by_id::number as note_updated_by_id,
        gitlab_dotcom_notes_dedupe_source.position::varchar as position_number,
        gitlab_dotcom_notes_dedupe_source.original_position::varchar
        as original_position,
        gitlab_dotcom_notes_dedupe_source.resolved_at::timestamp as resolved_at,
        gitlab_dotcom_notes_dedupe_source.resolved_by_id::number as resolved_by_id,
        gitlab_dotcom_notes_dedupe_source.discussion_id::varchar as discussion_id,
        gitlab_dotcom_notes_dedupe_source.cached_markdown_version::number
        as cached_markdown_version,
        gitlab_dotcom_notes_dedupe_source.resolved_by_push::boolean as resolved_by_push
    from gitlab_dotcom_notes_dedupe_source
    left join
        prep_project
        on gitlab_dotcom_notes_dedupe_source.project_id = prep_project.dim_project_id
    left join
        dim_epic on gitlab_dotcom_notes_dedupe_source.noteable_id = dim_epic.dim_epic_id
    left join dim_namespace on dim_epic.group_id = dim_namespace.dim_namespace_id
    left join
        dim_namespace_plan_hist
        on prep_project.ultimate_parent_namespace_id
        = dim_namespace_plan_hist.dim_namespace_id
        and gitlab_dotcom_notes_dedupe_source.created_at
        >= dim_namespace_plan_hist.valid_from
        and gitlab_dotcom_notes_dedupe_source.created_at
        < coalesce(dim_namespace_plan_hist.valid_to, '2099-01-01')
    left join
        prep_user on gitlab_dotcom_notes_dedupe_source.author_id = prep_user.dim_user_id
    left join
        dim_date
        on to_date(gitlab_dotcom_notes_dedupe_source.created_at) = dim_date.date_day

)

{{
    dbt_audit(
        cte_ref="joined",
        created_by="@mpeychet_",
        updated_by="@chrissharp",
        created_date="2021-06-22",
        updated_date="2022-03-29",
    )
}}
