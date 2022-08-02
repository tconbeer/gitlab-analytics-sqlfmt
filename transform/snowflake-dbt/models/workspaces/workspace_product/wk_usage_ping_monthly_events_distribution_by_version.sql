{{ config(tags=["mnpi_exception"]) }}

{{ config({"materialized": "incremental", "unique_key": "month_version_id"}) }}

with
    filtered_counters as (

        select *
        from {{ ref("mart_usage_ping_counters_statistics") }}
        where
            metrics_path ilike 'counts.%'
            and edition = 'CE'
            and first_major_version_with_counter between 1 and 12

    ),
    monthly_usage_data as (

        select *
        from {{ ref("monthly_usage_data") }}
        where
            monthly_metric_value > 0 and metrics_path ilike 'counts.%'
            {% if is_incremental() %}

            and created_month >= (select max(reporting_month) from {{ this }})

            {% endif %}
    ),
    dim_gitlab_releases as (select * from {{ ref("dim_gitlab_releases") }}),
    fct_usage_ping_payload as (select * from {{ ref("fct_usage_ping_payload") }}),
    outlier_detection_formula as (

        select
            created_month as reporting_month,
            metrics_path,
            (
                approx_percentile(monthly_metric_value, 0.75) - approx_percentile(
                    monthly_metric_value, 0.25
                )
            )
            * 3
            + approx_percentile(monthly_metric_value, 0.75) as outer_boundary
        from monthly_usage_data
        where
            monthly_metric_value > 0
            and metrics_path ilike 'counts.%'
            and created_month >= '2020-01-01'
        group by 1, 2

    ),
    joined as (

        select
            product_usage.created_month as reporting_month,
            fct_usage_ping_payload.major_minor_version,
            datediff(
                'month', date_trunc('month', release_date), product_usage.created_month
            ) as months_since_release,
            iff(
                fct_usage_ping_payload.edition = 'CE',
                'CE',
                iff(product_tier = 'Core', 'EE - Core', 'EE - Paid')
            ) as reworked_main_edition,
            sum(monthly_metric_value) as total_counts
        from monthly_usage_data as product_usage
        left join
            fct_usage_ping_payload
            on product_usage.ping_id = fct_usage_ping_payload.dim_usage_ping_id
        left join
            dim_gitlab_releases as release
            on fct_usage_ping_payload.major_minor_version = release.major_minor_version
        inner join
            filtered_counters
            on product_usage.metrics_path = filtered_counters.metrics_path
        inner join
            outlier_detection_formula
            on product_usage.metrics_path = outlier_detection_formula.metrics_path
            and product_usage.created_month = outlier_detection_formula.reporting_month
            and product_usage.monthly_metric_value <= outer_boundary
        where
            usage_ping_delivery_type = 'Self-Managed'
            and product_usage.created_month > '2020-01-01'
            and is_trial = false
        group by 1, 2, 3, 4

    ),
    data_with_unique_key as (

        select
            {{
                dbt_utils.surrogate_key(
                    ["reporting_month", "major_minor_version", "reworked_main_edition"]
                )
            }} as month_version_id, *
        from joined

    )

select *
from data_with_unique_key
