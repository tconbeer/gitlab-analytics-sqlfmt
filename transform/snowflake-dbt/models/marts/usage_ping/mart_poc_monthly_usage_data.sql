{{ config(tags=["mnpi_exception"]) }}

with
    fct_monthly_usage_data as (select * from {{ ref("monthly_usage_data") }}),
    prep_usage_ping_payload as (select * from {{ ref("prep_usage_ping_payload") }}),
    monthly_usage_data_agg as (

        select
            created_month,
            clean_metrics_name,
            instance_id,
            ping_id,
            group_name,
            stage_name,
            section_name,
            is_smau,
            is_gmau,
            is_paid_gmau,
            is_umau,
            max(monthly_metric_value) as monthly_metric_value
        from fct_monthly_usage_data
        where clean_metrics_name is not null {{ dbt_utils.group_by(n=11) }}

    )

select
    created_month,
    clean_metrics_name,
    edition,
    product_tier,
    group_name,
    stage_name,
    section_name,
    is_smau,
    is_gmau,
    is_paid_gmau,
    is_umau,
    usage_ping_delivery_type,
    sum(monthly_metric_value) as monthly_metric_value_sum
from monthly_usage_data_agg
inner join
    prep_usage_ping_payload
    on monthly_usage_data_agg.ping_id = prep_usage_ping_payload.dim_usage_ping_id
    {{ dbt_utils.group_by(n=12) }}
