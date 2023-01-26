{{ config(tags=["mnpi_exception"], materialized="incremental", unique_key="mau_id") }}


with
    date_details as (select * from {{ ref("date_details") }}),
    gitlab_dotcom_usage_data_events as (

        select *
        from {{ ref("gitlab_dotcom_usage_data_events") }}
        {% if is_incremental() %}

        where
            date_trunc('month', event_created_at)
            >= (select dateadd('days', -1, max(smau_month)) from {{ this }})

        {% endif %}

    ),
    gitlab_subscriptions as (

        select *
        from {{ ref("gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base") }}
    ),
    plans as (select * from {{ ref("gitlab_dotcom_plans") }})

select
    -- primary key
    {{
        dbt_utils.surrogate_key(
            [
                "gitlab_dotcom_usage_data_events.namespace_id",
                "user_id",
                "event_name",
                "DATEADD(month, -1, date_day)",
            ]
        )
    }} as mau_id,
    dateadd('month', -1, date_day) as smau_month,

    -- ids 
    gitlab_dotcom_usage_data_events.user_id,
    gitlab_dotcom_usage_data_events.namespace_id,

    -- user dimensions
    case
        when gitlab_subscriptions.is_trial
        then 'trial'
        else coalesce(gitlab_subscriptions.plan_id, 34)::varchar
    end as plan_id_at_smau_month_end,
    case
        when gitlab_subscriptions.is_trial
        then 'trial'
        else coalesce(plans.plan_name, 'free')
    end as plan_name_at_smau_month_end,

    -- event data
    event_name,
    stage_name,
    is_representative_of_stage,

    -- metadata
    -- aggregating because user_created_at and namespace_created_at can be impacted by
    -- late arriving dimensions
    max(datediff('day', user_created_at, date_day)) as days_since_user_creation,
    max(
        datediff('day', namespace_created_at, date_day)
    ) as days_since_namespace_creation,

    count(*) as event_count,
    count(distinct to_date(event_created_at)) as event_day_count
from date_details
inner join
    gitlab_dotcom_usage_data_events
    on gitlab_dotcom_usage_data_events.event_created_at
    between dateadd('day', -28, date_details.date_day) and date_day
left join
    gitlab_subscriptions
    on gitlab_dotcom_usage_data_events.namespace_id = gitlab_subscriptions.namespace_id
    -- taking the last day of the month
    and dateadd('day', -1, date_day)
    between gitlab_subscriptions.valid_from
    and {{ coalesce_to_infinity("gitlab_subscriptions.valid_to") }}
left join plans on gitlab_subscriptions.plan_id = plans.plan_id
where day_of_month = 1 {{ dbt_utils.group_by(n=9) }}
