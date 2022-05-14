{{ config(tags=["product", "mnpi_exception"], materialized="table") }}

{{ simple_cte([("mart_arr", "mart_arr")]) }}

-- Determine monthly sub and user count
,
subscription_info as (

    select
        {{ dbt_utils.surrogate_key(["arr_month"]) }}
        as rpt_service_ping_instance_subcription_opt_in_monthly_id,
        arr_month as arr_month,
        sum(arr) as arr,
        sum(quantity) as total_licensed_users,
        count(distinct dim_subscription_id) as total_subscription_count
    from mart_arr
    where product_tier_name != 'Storage' and product_delivery_type = 'Self-Managed'
    group by 1, 2
    order by 2 desc

)
{{
    dbt_audit(
        cte_ref="subscription_info",
        created_by="@icooper-acp",
        updated_by="@icooper-acp",
        created_date="2022-04-07",
        updated_date="2022-04-15",
    )
}}
