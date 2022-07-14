with
    source as (

        select
            *,
            rank() OVER (
                partition by date_trunc('day', uploaded_at) order by uploaded_at desc
            ) as rank
        from {{ source("gitlab_data_yaml", "stages") }}

    ),
    intermediate as (

        select
            d.value as data_by_row,
            date_trunc('day', uploaded_at)::date as snapshot_date,
            rank
        from
            source,
            lateral flatten(input => parse_json(jsontext['stages']), outer => true) d

    ),
    intermediate_stage as (

        select
            rank,
            data_by_row['related']::array as related_stages,
            snapshot_date,
            data_by_row['pm']::varchar as stage_product_manager,
            data_by_row['display_name']::varchar as stage_display_name,
            data_by_row['established']::number as stage_year_established,
            data_by_row['lifecycle']::number as stage_lifecycle,
            data_by_row['horizon']::varchar as stage_horizon,
            data_by_row['contributions']::number as stage_contributions,
            data_by_row['usage_driver_score']::number as stage_usage_driver_score,
            data_by_row['sam_driver_score']::number as stage_sam_driver_score,
            data_by_row['stage_development_spend_percent']::number
            as stage_development_spend_percent,
            data_by_row['groups']::array as stage_groups,
            data_by_row['section']::varchar as stage_section
        from intermediate

    )

select *
from intermediate_stage
