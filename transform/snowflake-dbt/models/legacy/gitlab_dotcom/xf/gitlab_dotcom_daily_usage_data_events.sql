{{ config(tags=["mnpi_exception"]) }}

{{ config({"materialized": "incremental", "unique_key": "daily_usage_data_event_id"}) }}

with
    usage_data as (

        select *
        from {{ ref("gitlab_dotcom_usage_data_events") }}
        {% if is_incremental() %}

        where
            event_created_at >= (
                select max(dateadd(day, -8, event_date)) from {{ this }}
            )

        {% endif %}

    ),
    aggregated as (

        select
            {{
                dbt_utils.surrogate_key(
                    [
                        "namespace_id",
                        "user_id",
                        "event_name",
                        "TO_DATE(event_created_at)",
                    ]
                )
            }} as daily_usage_data_event_id,
            namespace_id,
            is_blocked_namespace,
            namespace_created_at,
            user_id,
            namespace_is_internal,
            is_representative_of_stage,
            event_name,
            stage_name,
            plan_id_at_event_date,
            plan_name_at_event_date,
            plan_was_paid_at_event_date,
            user_created_at,
            to_date(event_created_at) as event_date,
            datediff(
                'day', to_date(namespace_created_at), event_date
            ) as days_since_namespace_creation,
            datediff(
                'week', to_date(namespace_created_at), event_date
            ) as weeks_since_namespace_creation,
            datediff(
                'day', to_date(user_created_at), event_date
            ) as days_since_user_creation,
            datediff(
                'week', to_date(user_created_at), event_date
            ) as weeks_since_user_creation,
            count(*) as event_count
        from usage_data
        where days_since_user_creation >= 0 {{ dbt_utils.group_by(n=18) }}

    )

select *
from aggregated
