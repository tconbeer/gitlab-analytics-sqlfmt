with
    source as (

        select *
        from {{ source("driveload", "ssa_quarterly_aggregated_metrics_for_coverage") }}

    )

select
    agg_key_value::varchar as agg_key_value,
    metric_name::varchar as metric_name,
    close_day_of_fiscal_quarter_normalised::number
    as close_day_of_fiscal_quarter_normalised,
    close_fiscal_quarter_name::varchar as close_fiscal_quarter_name,
    metric_value::number as metric_value,
    total_booked_net_arr::number as total_booked_net_arr,
    booked_net_arr::number as booked_net_arr,
    metric_coverage::number as metric_coverage,
    agg_key_name::varchar as agg_key_name

from source
