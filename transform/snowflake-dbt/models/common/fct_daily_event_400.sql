{{ config({"materialized": "incremental", "unique_key": "daily_usage_data_event_id"}) }}

{{
    simple_cte(
        [
            ("dim_namespace_plan_hist", "dim_namespace_plan_hist"),
        ]
    )
}}

,
usage_data as (

    select *
    from {{ ref("fct_event_400") }}
    {% if is_incremental() %}

    where
        event_created_at >= (
            select max(dateadd(day, -8, event_created_date)) from {{ this }}
        )

    {% endif %}

)

,
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
        ifnull(dim_namespace_plan_hist.dim_plan_id, 34) as dim_plan_id_at_event_date,

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
    left join
        dim_namespace_plan_hist
        on usage_data.ultimate_parent_namespace_id
        = dim_namespace_plan_hist.dim_namespace_id
        and to_date(
            usage_data.event_created_at
        ) >= dim_namespace_plan_hist.valid_from and to_date(
            usage_data.event_created_at
        ) < coalesce(dim_namespace_plan_hist.valid_to, '2099-01-01')
    where days_since_user_creation >= 0 {{ dbt_utils.group_by(n=14) }}

)

select *
from aggregated
