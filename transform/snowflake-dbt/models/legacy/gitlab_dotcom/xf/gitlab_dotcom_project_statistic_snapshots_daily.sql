{{ config(materialized="view") }}

with
    date_details as (

        select *
        from {{ ref("date_details") }}
        -- reduce size of results significantly
        where
            date_actual > '2020-03-01'
            and date_actual < {{ dbt_utils.current_timestamp() }}::date

    ),
    project_snapshots as (

        select *, ifnull(valid_to, current_timestamp) as valid_to_
        from {{ ref("gitlab_dotcom_project_statistics_snapshots_base") }}

    ),
    project_snapshots_daily as (

        select
            date_details.date_actual as snapshot_day,
            project_snapshots.project_statistics_id,
            project_snapshots.project_id,
            project_snapshots.namespace_id,
            project_snapshots.commit_count,
            project_snapshots.storage_size,
            project_snapshots.repository_size,
            project_snapshots.lfs_objects_size,
            project_snapshots.build_artifacts_size,
            project_snapshots.packages_size,
            project_snapshots.wiki_size,
            project_snapshots.shared_runners_seconds,
            project_snapshots.last_update_started_at
        from project_snapshots
        inner join
            date_details
            on date_details.date_actual
            between project_snapshots.valid_from::date and project_snapshots.valid_to_
            ::date
        qualify
            row_number() OVER (
                partition by snapshot_day, project_id order by valid_to_ desc
            )
            = 1

    )

select *
from project_snapshots_daily
