with
    counts as (

        select 'version_raw_usage_data_source' as model_name, count(*) as row_count
        from {{ ref("version_raw_usage_data_source") }}

        UNION ALL

        select 'version_usage_data_source' as model_name, count(*) as row_count
        from {{ ref("version_usage_data_source") }}

        UNION ALL

        select 'zuora_subscription_source' as model_name, count(*) as row_count
        from {{ ref("zuora_subscription_source") }}

        UNION ALL

        select 'zuora_rate_plan_source' as model_name, count(*) as row_count
        from {{ ref("zuora_rate_plan_source") }}

        UNION ALL

        select 'customers_db_orders_source' as model_name, count(*) as row_count
        from {{ ref("customers_db_orders_source") }}

        UNION ALL

        select
            'customers_db_license_seat_links_source' as model_name,
            count(*) as row_count
        from {{ ref("customers_db_license_seat_links_source") }}

    )


    {{
        dbt_audit(
            cte_ref="counts",
            created_by="@snalamaru",
            updated_by="@ischweickartDD",
            created_date="2021-02-19",
            updated_date="2021-04-05",
        )
    }}
