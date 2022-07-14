{{ config(materialized="incremental") }}

with
    dates as (select * from {{ ref("dim_date") }}),
    saas_usage_ping as (

        select *
        from {{ ref("prep_saas_usage_ping_namespace") }}
        where
            ping_date >= '2021-03-01'::date
            and ping_name like 'usage_activity_by_stage%'
            and counter_value > 0  -- Filter out non-instances
            -- Only return data for complete months
            and ping_date < date_trunc('month', current_date)

            {% if is_incremental() %}
            and date_trunc('month', ping_date)
            > (select max(snapshot_month) from {{ this }})
            {% endif %}

    ),
    saas_last_monthly_ping_per_account as (

        select
            saas_usage_ping.dim_namespace_id,
            dates.first_day_of_month as snapshot_month,
            saas_usage_ping.ping_name as metrics_path,
            saas_usage_ping.counter_value as metrics_value
        from saas_usage_ping
        inner join dates on saas_usage_ping.ping_date = dates.date_day
        qualify
            row_number() OVER (
                partition by
                    saas_usage_ping.dim_namespace_id,
                    dates.first_day_of_month,
                    saas_usage_ping.ping_name
                order by saas_usage_ping.ping_date desc
            )
            = 1

    ),
    flattened_metrics as (

        select dim_namespace_id, snapshot_month, metrics_path, metrics_value
        from saas_last_monthly_ping_per_account

    )

select *
from flattened_metrics
