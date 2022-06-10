{{ config(tags=["mnpi_exception"]) }}

{{ config({"materialized": "table"}) }}

{% set gainsight_wave_metrics = dbt_utils.get_column_values(
    table=ref("gainsight_wave_2_3_metrics"),
    column="metric_name",
    max_records=1000,
    default=[""],
) %}

{{
    simple_cte(
        [
            ("prep_saas_usage_ping_namespace", "prep_saas_usage_ping_namespace"),
            ("dim_date", "dim_date"),
            ("bdg_namespace_order", "bdg_namespace_order_subscription_monthly"),
            ("gainsight_wave_2_3_metrics", "gainsight_wave_2_3_metrics"),
        ]
    )
}}

,
free_namespaces as (

    select *
    from bdg_namespace_order
    where
        dim_namespace_id is not null and (dim_order_id is null or order_is_trial = true)

),
joined as (

    select
        prep_saas_usage_ping_namespace.dim_namespace_id,
        prep_saas_usage_ping_namespace.ping_date,
        prep_saas_usage_ping_namespace.ping_name,
        prep_saas_usage_ping_namespace.counter_value,
        dim_date.first_day_of_month as reporting_month,
        free_namespaces.dim_subscription_id,
        free_namespaces.dim_crm_account_id
    from prep_saas_usage_ping_namespace
    inner join dim_date on prep_saas_usage_ping_namespace.ping_date = dim_date.date_day
    inner join
        free_namespaces
        on prep_saas_usage_ping_namespace.dim_namespace_id
        = free_namespaces.dim_namespace_id
        and dim_date.first_day_of_month = free_namespaces.snapshot_month
    inner join
        gainsight_wave_2_3_metrics
        on prep_saas_usage_ping_namespace.ping_name
        = gainsight_wave_2_3_metrics.metric_name
    qualify
        row_number() over (
            partition by
                dim_date.first_day_of_month,
                prep_saas_usage_ping_namespace.dim_namespace_id,
                prep_saas_usage_ping_namespace.ping_name
            order by prep_saas_usage_ping_namespace.ping_date desc
        ) = 1

),
pivoted as (

    select
        dim_namespace_id,
        dim_subscription_id,
        dim_crm_account_id,
        reporting_month,
        max(ping_date) as ping_date,
        {{
            dbt_utils.pivot(
                "ping_name", gainsight_wave_metrics, then_value="counter_value"
            )
        }}
    from joined {{ dbt_utils.group_by(n=4) }}

)

{{
    dbt_audit(
        cte_ref="pivoted",
        created_by="@ischweickartDD",
        updated_by="@ischweickartDD",
        created_date="2021-06-04",
        updated_date="2021-06-04",
    )
}}
