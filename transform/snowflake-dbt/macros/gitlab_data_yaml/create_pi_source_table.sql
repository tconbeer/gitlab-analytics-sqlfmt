{%- macro create_pi_source_table(source_performance_indicator) -%}

with
    source as (select * from {{ source_performance_indicator }}),
    intermediate as (

        select
            d.value as data_by_row,
            date_trunc('day', uploaded_at)::date as snapshot_date
        from source, lateral flatten(input => parse_json(jsontext), outer => true) d

    ),
    renamed as (

        select
            data_by_row['name']::varchar as pi_name,
            data_by_row['org']::varchar as org_name,
            data_by_row['definition']::varchar as pi_definition,
            data_by_row['is_key']::boolean as is_key,
            data_by_row['is_primary']::boolean as is_primary,
            data_by_row['public']::boolean as is_public,
            data_by_row['sisense_data'] is not null as is_embedded,
            data_by_row['target']::varchar as pi_target,
            data_by_row['target_name']::varchar as pi_metric_target_name,
            data_by_row['monthly_recorded_targets']::varchar
            as pi_monthly_recorded_targets,
            data_by_row['monthly_estimated_targets']::varchar
            as pi_monthly_estimated_targets,
            data_by_row['metric_name']::varchar as pi_metric_name,
            data_by_row['telemetry_type']::varchar as telemetry_type,
            data_by_row['urls']::varchar as pi_url,
            data_by_row['sisense_data'].chart::varchar as sisense_chart_id,
            data_by_row['sisense_data'].dashboard::varchar as sisense_dashboard_id,
            snapshot_date
        from intermediate

    ),
    intermediate_stage as (

        select
            {{
                dbt_utils.surrogate_key(
                    [
                        "pi_name",
                        "org_name",
                        "pi_definition",
                        "is_key",
                        "is_public",
                        "is_embedded",
                        "pi_target",
                        "pi_metric_target_name",
                        "pi_monthly_recorded_targets",
                        "pi_monthly_estimated_targets",
                        "pi_url",
                    ]
                )
            }} as unique_key, renamed.*
        from renamed

    ),
    final as (

        select
            *,
            first_value(snapshot_date) over (
                partition by pi_name order by snapshot_date
            ) as date_first_added,
            min(snapshot_date) over (
                partition by unique_key order by snapshot_date
            ) as valid_from_date,
            max(snapshot_date) over (
                partition by unique_key order by snapshot_date desc
            ) as valid_to_date
        from intermediate_stage

    )

select *
from final

{% endmacro %}
