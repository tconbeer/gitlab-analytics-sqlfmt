with
    greenhouse_metrics_unpivoted as (

        {{
            dbt_utils.unpivot(
                relation=ref("greenhouse_sourcer_base"),
                cast_to="FLOAT",
                exclude=[
                    "reporting_month",
                    "start_period",
                    "end_period",
                    "sourcer_name",
                    "month_date",
                ],
            )
        }}

    ),
    sourcer_metrics as (

        {{
            dbt_utils.unpivot(
                relation=ref("greenhouse_sourcer_metrics"),
                cast_to="FLOAT",
                exclude=["month_date", "sourcer_name", "part_of_recruiting_team"],
            )
        }}

    ),
    outlier as (

        select
            reporting_month,
            field_name,
            percentile_cont(0.1) within group(order by value) as outlier
        from greenhouse_metrics_unpivoted
        where value is not null
        group by 1, 2

    ),
    baseline as (

        select
            greenhouse_metrics_unpivoted.reporting_month,
            greenhouse_metrics_unpivoted.field_name,
            percentile_cont(0.25) within group(order by value) as percentile_25th,
            percentile_cont(0.50) within group(order by value) as percentile_50th,
            percentile_cont(0.75) within group(order by value) as percentile_75th,
            percentile_cont(0.80) within group(order by value) as percentile_80th,
            percentile_cont(0.90) within group(order by value) as ninetieth_percentile,
            percentile_cont(1.00) within group(order by value) as percentile_max
        from greenhouse_metrics_unpivoted
        left join
            outlier
            on greenhouse_metrics_unpivoted.reporting_month = outlier.reporting_month
            and greenhouse_metrics_unpivoted.field_name = outlier.field_name
        where greenhouse_metrics_unpivoted.value > outlier.outlier
        -- --removing outliers when identifying the percentiles to use---
        group by 1, 2

    ),
    final as (

        select
            sourcer_metrics.month_date,
            sourcer_metrics.sourcer_name,
            sourcer_metrics.field_name as recruiting_metric,
            sourcer_metrics.value as recruiting_metric_value,
            baseline.percentile_25th,
            baseline.percentile_50th,
            baseline.percentile_75th,
            baseline.percentile_80th,
            baseline.ninetieth_percentile,
            baseline.percentile_max
        from sourcer_metrics
        left join
            baseline
            on sourcer_metrics.month_date = baseline.reporting_month
            and sourcer_metrics.field_name = baseline.field_name

    )

select *
from final
