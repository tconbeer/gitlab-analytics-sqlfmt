{{ config(materialized="view") }}

with
    date_details as (

        select * from {{ ref("date_details") }} where last_day_of_month = date_actual

    ),
    namespace_statistics_snapshots as (

        select *, ifnull(valid_to, current_timestamp) as valid_to_
        from {{ ref("gitlab_dotcom_namespace_statistics_snapshots_base") }}

    ),
    namespace_statistics_snapshots_monthly as (

        select
            date_trunc('month', date_details.date_actual) as snapshot_month,
            namespace_statistics_snapshots.namespace_id,
            namespace_statistics_snapshots.shared_runners_seconds,
            namespace_statistics_snapshots.shared_runners_seconds_last_reset
        from namespace_statistics_snapshots
        inner join
            date_details
            on date_details.date_actual
            between namespace_statistics_snapshots.valid_from and namespace_statistics_snapshots.valid_to_
        qualify
            row_number() over (
                partition by snapshot_month, namespace_id order by valid_to_ desc
            )
            = 1

    )

select *
from namespace_statistics_snapshots_monthly
