{{ config(tags=["mnpi_exception"]) }}

with
    seat_links as (

        select *, date_trunc('month', report_date) as snapshot_month
        from {{ ref("prep_usage_self_managed_seat_link") }}
        qualify
            row_number() OVER (
                partition by order_subscription_id, snapshot_month
                order by report_date desc
            )
            = 1

    ),
    final as (

        select
            -- ids & keys
            customers_db_order_id as latest_order_id_in_month,
            dim_subscription_id,
            dim_subscription_id_original,
            dim_subscription_id_previous,
            dim_crm_account_id,
            dim_billing_account_id,
            dim_product_tier_id,

            -- counts
            seat_links.active_user_count as active_user_count,
            seat_links.license_user_count,
            max_historical_user_count,

            -- flags
            is_last_seat_link_report_per_subscription,
            is_last_seat_link_report_per_order,
            is_subscription_in_zuora,
            is_rate_plan_in_zuora,
            is_active_user_count_available,

            -- dates
            seat_links.snapshot_month,
            seat_links.report_date
        from seat_links

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@ischweickartDD",
            updated_by="@ischweickartDD",
            created_date="2021-01-11",
            updated_date="2021-02-08",
        )
    }}
