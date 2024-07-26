{{ config({"materialized": "incremental", "unique_key": "event_id"}) }}

with
    source as (

        select *
        from {{ ref("gitlab_dotcom_events_source") }}
        {% if is_incremental() %}

            where updated_at >= (select max(updated_at) from {{ this }})

        {% endif %}

    ),
    projects as (select * from {{ ref("gitlab_dotcom_projects_xf") }}),
    gitlab_subscriptions as (

        select *
        from {{ ref("gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base") }}

    ),
    plans as (select * from {{ ref("gitlab_dotcom_plans") }}),
    users as (

        select *
        from {{ ref("gitlab_dotcom_users") }} users
        where {{ filter_out_blocked_users("users", "user_id") }}

    ),
    joined as (

        select
            source.*,
            projects.ultimate_parent_id,
            case
                when gitlab_subscriptions.is_trial
                then 'trial'
                else coalesce(gitlab_subscriptions.plan_id, 34)::varchar
            end as plan_id_at_event_date,
            case
                when gitlab_subscriptions.is_trial
                then 'trial'
                else coalesce(plans.plan_name, 'free')
            end as plan_name_at_event_date,
            coalesce(plans.plan_is_paid, false) as plan_was_paid_at_event_date,
            users.created_at as user_created_at
        from source
        left join projects on source.project_id = projects.project_id
        left join
            gitlab_subscriptions
            on projects.ultimate_parent_id = gitlab_subscriptions.namespace_id
            and source.created_at
            between gitlab_subscriptions.valid_from
            and {{ coalesce_to_infinity("gitlab_subscriptions.valid_to") }}
        left join plans on gitlab_subscriptions.plan_id = plans.plan_id
        left join users on source.author_id = users.user_id

    )

select *
from joined
order by updated_at
