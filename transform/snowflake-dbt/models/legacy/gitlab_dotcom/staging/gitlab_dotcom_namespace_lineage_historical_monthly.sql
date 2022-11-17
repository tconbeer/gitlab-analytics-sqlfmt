{{ config(materialized="view") }}

with
    date_details as (

        select * from {{ ref("date_details") }} where last_day_of_month = date_actual

    ),
    namespace_lineage_snapshots_daily as (

        select * from {{ ref("gitlab_dotcom_namespace_lineage_historical_daily") }}

    ),
    namespace_lineage_snapshots_monthly as (

        select
            date_details.first_day_of_month as snapshot_month,
            namespace_lineage_snapshots_daily.namespace_id,
            namespace_lineage_snapshots_daily.parent_id,
            namespace_lineage_snapshots_daily.upstream_lineage,
            namespace_lineage_snapshots_daily.ultimate_parent_id,
            namespace_lineage_snapshots_daily.namespace_is_internal,
            namespace_lineage_snapshots_daily.ultimate_parent_plan_id
        from namespace_lineage_snapshots_daily
        inner join
            date_details
            on date_details.date_actual = namespace_lineage_snapshots_daily.snapshot_day

    )

select *
from namespace_lineage_snapshots_monthly
