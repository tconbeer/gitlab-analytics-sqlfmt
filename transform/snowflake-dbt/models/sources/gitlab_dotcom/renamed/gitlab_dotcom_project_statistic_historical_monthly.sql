with
    date_details as (

        select * from {{ ref("date_details") }} where last_day_of_month = date_actual

    ),
    project_snapshots as (

        select *, ifnull(valid_to, current_timestamp) as valid_to_
        from {{ ref("gitlab_dotcom_project_statistics_snapshots_base") }}

    ),
    project_snapshots_monthly as (

        select
            date_trunc('month', date_details.date_actual) as snapshot_month,
            project_snapshots.project_statistics_id,
            project_snapshots.project_id,
            project_snapshots.namespace_id,
            project_snapshots.commit_count,
            project_snapshots.storage_size,
            project_snapshots.repository_size,
            project_snapshots.lfs_objects_size,
            project_snapshots.build_artifacts_size,
            project_snapshots.shared_runners_seconds,
            project_snapshots.last_update_started_at
        from project_snapshots
        inner join
            date_details
            on date_details.date_actual
            between project_snapshots.valid_from and project_snapshots.valid_to_
        qualify
            row_number() over (
                partition by snapshot_month, project_id order by valid_to_ desc
            )
            = 1

    )

select *
from project_snapshots_monthly
