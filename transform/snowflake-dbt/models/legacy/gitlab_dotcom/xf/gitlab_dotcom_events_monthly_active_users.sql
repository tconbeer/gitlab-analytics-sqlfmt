with
    days as (

        select distinct
            date_day as day, (date_day = last_day_of_month) as is_last_day_of_month
        from {{ ref("date_details") }}
        where date_day < current_date

    ),
    audit_events as (

        select distinct author_id, to_date(created_at) as audit_event_day
        from {{ ref("gitlab_dotcom_audit_events") }}
        where true

    ),
    events as (

        select distinct
            author_id,
            ultimate_parent_id,
            plan_id_at_event_date,
            plan_was_paid_at_event_date,
            to_date(created_at) as event_day
        from {{ ref("gitlab_dotcom_events") }} dotcom_events
        where {{ filter_out_blocked_users("dotcom_events", "author_id") }}

    ),
    audit_events_active_user as (

        select
            days.day,
            days.is_last_day_of_month,
            count(distinct author_id) as count_audit_events_active_users_last_28_days
        from days
        inner join
            audit_events
            on audit_event_day between dateadd('day', -27, days.day) and days.day
        group by days.day, days.is_last_day_of_month
        order by days.day

    ),
    events_active_user as (

        select distinct
            days.day,
            days.is_last_day_of_month,
            events.plan_id_at_event_date,
            events.plan_was_paid_at_event_date,
            count(distinct author_id) over (
                partition by days.day
            ) as count_events_active_users_last_28_days,
            count(distinct author_id) over (
                partition by days.day, events.plan_id_at_event_date
            ) as count_events_active_users_last_28_days_by_plan_id,
            count(distinct ultimate_parent_id) over (
                partition by days.day, events.plan_id_at_event_date
            ) as count_events_active_namespaces_last_28_days_by_plan_id,
            count(distinct author_id) over (
                partition by days.day, events.plan_was_paid_at_event_date
            ) as count_events_active_users_last_28_days_by_plan_was_paid
        from days
        inner join
            events
            on events.event_day between dateadd('day', -27, days.day) and days.day
        order by days.day

    ),
    joined as (

        select distinct
            audit_events_active_user.day,
            audit_events_active_user.is_last_day_of_month,
            audit_events_active_user.count_audit_events_active_users_last_28_days,
            events_active_user.plan_id_at_event_date,
            events_active_user.plan_was_paid_at_event_date,
            events_active_user.count_events_active_users_last_28_days,
            events_active_user.count_events_active_users_last_28_days_by_plan_id,
            events_active_user.count_events_active_namespaces_last_28_days_by_plan_id,
            events_active_user.count_events_active_users_last_28_days_by_plan_was_paid
        from audit_events_active_user
        left join
            events_active_user on audit_events_active_user.day = events_active_user.day
    )

select *
from joined
