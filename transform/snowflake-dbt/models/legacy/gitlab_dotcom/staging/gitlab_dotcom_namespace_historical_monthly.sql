{{ config(materialized="view") }}

with
    date_details as (

        select * from {{ ref("date_details") }} where last_day_of_month = date_actual

    ),
    namespace_snapshots_daily as (

        select * from {{ ref("gitlab_dotcom_namespace_historical_daily") }}

    ),
    namespace_snapshots_monthly as (

        select
            date_details.first_day_of_month as snapshot_month,
            namespace_snapshots_daily.namespace_id,
            namespace_snapshots_daily.parent_id,
            namespace_snapshots_daily.owner_id,
            namespace_snapshots_daily.namespace_type,
            namespace_snapshots_daily.visibility_level,
            namespace_snapshots_daily.shared_runners_minutes_limit,
            namespace_snapshots_daily.extra_shared_runners_minutes_limit,
            namespace_snapshots_daily.repository_size_limit
        from namespace_snapshots_daily
        inner join
            date_details
            on date_details.date_actual = namespace_snapshots_daily.snapshot_day

    )

select *
from namespace_snapshots_monthly
