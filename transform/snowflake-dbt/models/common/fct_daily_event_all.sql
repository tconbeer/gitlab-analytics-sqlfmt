{{ config({"materialized": "incremental", "unique_key": "daily_usage_data_event_id"}) }}


with
    usage_data as (

        select *
        from {{ ref("fct_event_all") }}
        {% if is_incremental() %}

            where
                event_created_at
                >= (select max(dateadd(day, -8, event_created_date)) from {{ this }})

        {% endif %}

    ),
    aggregated as (

        select
            -- PRIMARY KEY
            {{
                dbt_utils.surrogate_key(
                    [
                        "ultimate_parent_namespace_id",
                        "dim_user_id",
                        "event_name",
                        "event_created_at",
                    ]
                )
            }} as daily_usage_data_event_id,

            -- FOREIGN KEY
            ultimate_parent_namespace_id,
            dim_user_id,
            event_name,
            to_date(event_created_at) as event_created_date,

            is_blocked_namespace_creator,
            namespace_created_date,
            namespace_is_internal,
            user_created_date,
            datediff(
                'day', namespace_created_date, event_created_date
            ) as days_since_namespace_creation,
            datediff(
                'week', namespace_created_date, event_created_date
            ) as weeks_since_namespace_creation,
            datediff(
                'day', user_created_date, event_created_date
            ) as days_since_user_creation,
            datediff(
                'week', user_created_date, event_created_date
            ) as weeks_since_user_creation,
            count(distinct event_id) as event_count
        from usage_data
        where days_since_user_creation >= 0 {{ dbt_utils.group_by(n=13) }}

    )

select *
from aggregated
