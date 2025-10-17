{{ config(tags=["product"]) }}

{{
    simple_cte(
        [
            ("dim_namespace_plan_hist", "dim_namespace_plan_hist"),
            ("plans", "gitlab_dotcom_plans_source"),
            ("prep_project", "prep_project"),
            ("gitlab_dotcom_events_source", "gitlab_dotcom_events_dedupe_source"),
            ("dim_date", "dim_date"),
        ]
    )
}},
prep_user as (select * from {{ ref("prep_user") }} users where user_state <> 'blocked'),
joined as (

    select
        gitlab_dotcom_events_source.id as dim_action_id,

        -- FOREIGN KEYS
        gitlab_dotcom_events_source.project_id::number as dim_project_id,
        prep_project.dim_namespace_id,
        prep_project.ultimate_parent_namespace_id,
        prep_user.dim_user_id,
        dim_date.date_id as created_date_id,
        ifnull(dim_namespace_plan_hist.dim_plan_id, 34) as dim_plan_id,

        -- events metadata
        gitlab_dotcom_events_source.target_id::number as target_id,
        gitlab_dotcom_events_source.target_type::varchar as target_type,
        gitlab_dotcom_events_source.created_at::timestamp as created_at,
        {{ action_type(action_type_id="action") }}::varchar as event_action_type
    from gitlab_dotcom_events_source
    left join
        prep_project
        on gitlab_dotcom_events_source.project_id = prep_project.dim_project_id
    left join
        dim_namespace_plan_hist
        on prep_project.ultimate_parent_namespace_id
        = dim_namespace_plan_hist.dim_namespace_id
        and gitlab_dotcom_events_source.created_at >= dim_namespace_plan_hist.valid_from
        and gitlab_dotcom_events_source.created_at
        < coalesce(dim_namespace_plan_hist.valid_to, '2099-01-01')
    left join prep_user on gitlab_dotcom_events_source.author_id = prep_user.dim_user_id
    left join
        dim_date on to_date(gitlab_dotcom_events_source.created_at) = dim_date.date_day

)

select *
from joined
