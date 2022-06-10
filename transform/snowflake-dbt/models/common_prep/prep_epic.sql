{{ config(tags=["product"]) }}

{{ config({"materialized": "incremental", "unique_key": "dim_epic_id"}) }}

{{
    simple_cte(
        [
            ("dim_date", "dim_date"),
            ("dim_namespace_plan_hist", "dim_namespace_plan_hist"),
            ("plans", "gitlab_dotcom_plans_source"),
            ("dim_namespace", "dim_namespace"),
            ("gitlab_dotcom_routes_source", "gitlab_dotcom_routes_source"),
            ("prep_label_links", "prep_label_links"),
            ("prep_labels", "prep_labels"),
            ("gitlab_dotcom_award_emoji_source", "gitlab_dotcom_award_emoji_source"),
        ]
    )
}}

,
gitlab_dotcom_epics_dedupe_source as (

    select *
    from {{ ref("gitlab_dotcom_epics_dedupe_source") }}
    {% if is_incremental() %}

    where updated_at >= (select max(updated_at) from {{ this }})

    {% endif %}

),
prep_user as (

    select *
    from {{ ref("prep_user") }} users
    where {{ filter_out_blocked_users("users", "dim_user_id") }}

),
upvote_count as (

    select
        awardable_id as dim_epic_id,
        sum(iff(award_emoji_name like 'thumbsup%', 1, 0)) as thumbsups_count,
        sum(iff(award_emoji_name like 'thumbsdown%', 1, 0)) as thumbsdowns_count,
        thumbsups_count - thumbsdowns_count as upvote_count
    from gitlab_dotcom_award_emoji_source
    where awardable_type = 'Epic'
    group by 1

),
agg_labels as (

    select
        prep_label_links.dim_epic_id as dim_epic_id,
        array_agg(lower(prep_labels.label_title)) within group(
            order by prep_labels.label_title asc
        ) as labels
    from prep_label_links
    left join prep_labels on prep_label_links.dim_label_id = prep_labels.dim_label_id
    where prep_label_links.dim_epic_id is not null
    group by 1

),
joined as (

    select
        gitlab_dotcom_epics_dedupe_source.id::number as dim_epic_id,
        gitlab_dotcom_epics_dedupe_source.author_id::number as author_id,
        gitlab_dotcom_epics_dedupe_source.group_id::number as group_id,
        dim_namespace.ultimate_parent_namespace_id::number
        as ultimate_parent_namespace_id,
        dim_date.date_id::number as created_date_id,
        ifnull(dim_namespace_plan_hist.dim_plan_id, 34)::number as dim_plan_id,
        gitlab_dotcom_epics_dedupe_source.assignee_id::number as assignee_id,
        gitlab_dotcom_epics_dedupe_source.iid::number as epic_internal_id,
        gitlab_dotcom_epics_dedupe_source.updated_by_id::number as updated_by_id,
        gitlab_dotcom_epics_dedupe_source.last_edited_by_id::number
        as last_edited_by_id,
        gitlab_dotcom_epics_dedupe_source.lock_version::number as lock_version,
        gitlab_dotcom_epics_dedupe_source.start_date::date as epic_start_date,
        gitlab_dotcom_epics_dedupe_source.end_date::date as epic_end_date,
        gitlab_dotcom_epics_dedupe_source.last_edited_at::timestamp
        as epic_last_edited_at,
        gitlab_dotcom_epics_dedupe_source.created_at::timestamp as created_at,
        gitlab_dotcom_epics_dedupe_source.updated_at::timestamp as updated_at,
        iff(
            dim_namespace.visibility_level = 'private',
            'private - masked',
            gitlab_dotcom_epics_dedupe_source.title::varchar
        ) as epic_title,
        gitlab_dotcom_epics_dedupe_source.description::varchar as epic_description,
        gitlab_dotcom_epics_dedupe_source.closed_at::timestamp as closed_at,
        gitlab_dotcom_epics_dedupe_source.state_id::number as state_id,
        gitlab_dotcom_epics_dedupe_source.parent_id::number as parent_id,
        gitlab_dotcom_epics_dedupe_source.relative_position::number
        as relative_position,
        gitlab_dotcom_epics_dedupe_source.start_date_sourcing_epic_id::number
        as start_date_sourcing_epic_id,
        gitlab_dotcom_epics_dedupe_source.external_key::varchar as external_key,
        gitlab_dotcom_epics_dedupe_source.confidential::boolean as is_confidential,
        {{ map_state_id("gitlab_dotcom_epics_dedupe_source.state_id") }} as state_name,
        length(gitlab_dotcom_epics_dedupe_source.title)::number as epic_title_length,
        length(
            gitlab_dotcom_epics_dedupe_source.description
        )::number as epic_description_length,
        iff(
            dim_namespace.visibility_level = 'private',
            'private - masked',
            'https://gitlab.com/groups/'
            || gitlab_dotcom_routes_source.path
            || '/-/epics/'
            || gitlab_dotcom_epics_dedupe_source.iid
        ) as epic_url,
        iff(
            dim_namespace.visibility_level = 'private',
            array_construct('private - masked'),
            agg_labels.labels
        ) as labels,
        ifnull(upvote_count.upvote_count, 0) as upvote_count
    from gitlab_dotcom_epics_dedupe_source
    left join
        dim_namespace
        on gitlab_dotcom_epics_dedupe_source.group_id = dim_namespace.dim_namespace_id
    left join
        dim_namespace_plan_hist
        on dim_namespace.ultimate_parent_namespace_id
        = dim_namespace_plan_hist.dim_namespace_id
        and gitlab_dotcom_epics_dedupe_source.created_at
        >= dim_namespace_plan_hist.valid_from
        and gitlab_dotcom_epics_dedupe_source.created_at < coalesce(
            dim_namespace_plan_hist.valid_to, '2099-01-01'
        )
    left join
        prep_user on gitlab_dotcom_epics_dedupe_source.author_id = prep_user.dim_user_id
    left join
        dim_date on to_date(
            gitlab_dotcom_epics_dedupe_source.created_at
        ) = dim_date.date_day
    left join
        gitlab_dotcom_routes_source
        on gitlab_dotcom_routes_source.source_id
        = gitlab_dotcom_epics_dedupe_source.group_id
        and gitlab_dotcom_routes_source.source_type = 'Namespace'
    left join
        agg_labels on agg_labels.dim_epic_id = gitlab_dotcom_epics_dedupe_source.id
    left join
        upvote_count on upvote_count.dim_epic_id = gitlab_dotcom_epics_dedupe_source.id

)

{{
    dbt_audit(
        cte_ref="joined",
        created_by="@mpeychet_",
        updated_by="@jpeguero",
        created_date="2021-06-22",
        updated_date="2021-10-24",
    )
}}
