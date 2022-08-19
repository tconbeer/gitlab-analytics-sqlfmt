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
            (
                "bdg_namespace_subscription",
                "bdg_namespace_order_subscription_monthly",
            ),
            ("gainsight_wave_2_3_metrics", "gainsight_wave_2_3_metrics"),
            ("instance_types", "dim_host_instance_type"),
        ]
    )
}},
joined as (

    select
        prep_saas_usage_ping_namespace.dim_namespace_id,
        prep_saas_usage_ping_namespace.ping_date,
        prep_saas_usage_ping_namespace.ping_name,
        prep_saas_usage_ping_namespace.counter_value,
        dim_date.first_day_of_month as reporting_month,
        bdg_namespace_subscription.dim_subscription_id,
        instance_types.instance_type
    from prep_saas_usage_ping_namespace
    left join
        instance_types
        on prep_saas_usage_ping_namespace.dim_namespace_id = instance_types.namespace_id
    inner join dim_date on prep_saas_usage_ping_namespace.ping_date = dim_date.date_day
    inner join
        bdg_namespace_subscription
        on prep_saas_usage_ping_namespace.dim_namespace_id
        = bdg_namespace_subscription.dim_namespace_id
        and dim_date.first_day_of_month = bdg_namespace_subscription.snapshot_month
        and namespace_order_subscription_match_status = 'Paid All Matching'
    inner join
        gainsight_wave_2_3_metrics
        on prep_saas_usage_ping_namespace.ping_name
        = gainsight_wave_2_3_metrics.metric_name
    qualify
        row_number() over (
            partition by
                dim_date.first_day_of_month,
                bdg_namespace_subscription.dim_subscription_id,
                prep_saas_usage_ping_namespace.dim_namespace_id,
                prep_saas_usage_ping_namespace.ping_name
            order by prep_saas_usage_ping_namespace.ping_date desc
        )
        = 1

),
pivoted as (

    select
        dim_namespace_id,
        dim_subscription_id,
        reporting_month,
        instance_type,
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
        created_by="@mpeychet_",
        updated_by="@snalamaru",
        created_date="2021-03-22",
        updated_date="2021-10-07",
    )
}}
