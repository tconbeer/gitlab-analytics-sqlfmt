with
    date_details as (

        select * from {{ ref("date_details") }} where last_day_of_month = date_actual

    ),
    project_snapshots as (
        select *, ifnull(valid_to, current_timestamp) as valid_to_
        from {{ ref("gitlab_dotcom_projects_snapshots_base") }}

    ),
    project_snapshots_monthly as (

        select
            date_trunc('month', date_details.date_actual) as snapshot_month,
            project_snapshots.project_id,
            project_snapshots.namespace_id,
            project_snapshots.visibility_level,
            project_snapshots.shared_runners_enabled
        from project_snapshots
        inner join
            date_details
            on date_details.date_actual
            between project_snapshots.valid_from and project_snapshots.valid_to_
        qualify
            row_number() OVER (
                partition by snapshot_month, project_id order by valid_to_ desc
            )
            = 1

    )

select *
from project_snapshots_monthly
