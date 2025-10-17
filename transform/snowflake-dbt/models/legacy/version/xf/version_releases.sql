with
    usage_data as (select * from {{ ref("version_usage_data") }}),

    release_schedule as (select * from {{ ref("dim_gitlab_releases") }}),

    aggregated as (

        select

            release_schedule.major_minor_version,
            release_schedule.release_date,
            release_schedule.major_version,
            release_schedule.minor_version,

            row_number() over (
                order by release_schedule.release_date
            ) as version_row_number,
            lead(release_schedule.release_date) over (
                order by release_schedule.release_date
            ) as next_version_release_date,

            min(
                iff(
                    usage_data.version_is_prerelease = false,
                    usage_data.created_at,
                    null
                )
            ) as min_usage_ping_created_at,
            max(
                iff(
                    usage_data.version_is_prerelease = false,
                    usage_data.created_at,
                    null
                )
            ) as max_usage_ping_created_at

        from release_schedule
        left join
            usage_data
            on usage_data.major_minor_version = release_schedule.major_minor_version
        group by 1, 2, 3, 4
        order by 3, 4

    )

select *
from aggregated
