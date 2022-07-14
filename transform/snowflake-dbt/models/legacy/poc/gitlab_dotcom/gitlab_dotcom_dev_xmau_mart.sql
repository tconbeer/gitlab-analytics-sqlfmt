{{ config(tags=["mnpi_exception"]) }}

with
    skeleton as (

        select 
      distinct first_day_of_month, last_day_of_month
        from {{ ref("date_details") }}
        where date_day = last_day_of_month and last_day_of_month < current_date()

    ),
    gitlab_dotcom_xmau_metrics as (

        select * from {{ ref("gitlab_dotcom_xmau_metrics") }}),
    events as (

        select
            user_id,
            namespace_id,
            event_date,
            plan_name_at_event_date,
            plan_id_at_event_date,
            plan_was_paid_at_event_date,
            namespace_is_internal,
            xmau.event_name as event_name,
            xmau.stage_name as stage_name,
            xmau.smau::boolean as is_smau,
            xmau.group_name as group_name,
            xmau.gmau::boolean as is_gmau,
            xmau.section_name::varchar as section_name,
            xmau.is_umau::boolean as is_umau
        from {{ ref("gitlab_dotcom_daily_usage_data_events") }} as events
        inner join
            gitlab_dotcom_xmau_metrics as xmau
            on events.event_name = xmau.events_to_include

    ),
    joined as (

        select
            first_day_of_month,
            event_name,
            stage_name,
            is_smau,
            group_name,
            is_gmau,
            section_name,
            is_umau,
            count(distinct user_id) as total_user_count,
            count(
                distinct iff(plan_was_paid_at_event_date = false, user_id, null)
            ) as free_user_count,
            count(
                distinct iff(plan_was_paid_at_event_date = true, user_id, null)
            ) as paid_user_count,
            count(distinct namespace_id) as total_namespace_count,
            count(
                distinct iff(plan_was_paid_at_event_date = false, namespace_id, null)
            ) as free_namespace_count,
            count(
                distinct iff(plan_was_paid_at_event_date = true, namespace_id, null)
            ) as paid_namespace_count
        from skeleton
        left join
            events
            on event_date between dateadd(
                'days', -28, last_day_of_month
            ) and last_day_of_month
            {{ dbt_utils.group_by(n=8) }}

    )

select *
from joined
