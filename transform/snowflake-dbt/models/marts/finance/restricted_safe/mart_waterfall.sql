{{ config({"materialized": "table", "transient": false}) }}

{{
    simple_cte(
        [
            ("rcl", "zuora_revenue_revenue_contract_line_source"),
            ("act", "zuora_revenue_accounting_type_source"),
            ("rc", "zuora_revenue_revenue_contract_header_source"),
            ("pob", "zuora_revenue_revenue_contract_performance_obligation_source"),
            ("rb", "zuora_revenue_book_source"),
            ("org", "zuora_revenue_organization_source"),
            ("cal", "zuora_revenue_calendar_source"),
            (
                "deleted_schedules",
                "zuora_revenue_revenue_contract_schedule_deleted_source",
            ),
            ("zuora_account", "zuora_account_source"),
            ("zuora_contact_source", "zuora_contact_source"),
        ]
    )
}}

,
schd as (

    select zuora_revenue_revenue_contract_schedule_source.*
    from "PREP".zuora_revenue.zuora_revenue_revenue_contract_schedule_source
    left join
        deleted_schedules
        on zuora_revenue_revenue_contract_schedule_source.revenue_contract_schedule_id
        = deleted_schedules.revenue_contract_schedule_id
    where deleted_schedules.revenue_contract_schedule_id is null

),
waterfall_summary as (

    select
        cal.period_id as as_of_period_id,
        schd.revenue_contract_schedule_created_period_id,
        schd.revenue_contract_schedule_id,
        schd.revenue_contract_id,
        schd.revenue_contract_line_id,
        schd.root_line_id,
        schd.period_id as period_id,
        schd.posted_period_id,
        schd.security_attribute_value,
        schd.book_id,
        schd.client_id,
        schd.accounting_segment,
        schd.accounting_type_id,
        schd.is_netting_entry,
        schd.schedule_type,
        schd.amount as t_at,
        schd.amount * schd.functional_currency_exchange_rate as f_at,
        (
            schd.amount * schd.functional_currency_exchange_rate
        ) * schd.reporting_currency_exchange_rate as r_at,
        schd.revenue_contract_schedule_created_date,
        schd.revenue_contract_schedule_created_by,
        schd.revenue_contract_schedule_updated_date,
        schd.revenue_contract_schedule_updated_by,
        schd.revenue_contract_schedule_updated_date as incremental_update_date
    from schd
    inner join
        cal
        on schd.revenue_contract_schedule_created_period_id <= cal.period_id
        and schd.period_id >= cal.period_id
    inner join act on schd.accounting_type_id = act.accounting_type_id
    where act.is_waterfall_account = 'Y' and act.is_cost = 'N'

),
waterfall as (

    select
        wf.as_of_period_id,
        wf.period_id,
        rb.book_name,
        org.organization_name,
        rc.revenue_contract_id,
        pob.revenue_contract_performance_obligation_name,
        wf.revenue_contract_line_id,
        coalesce(
            rcl.customer_name, zuora_account.account_name, rc.customer_name
        ) as revenue_contract_customer_name,
        rcl.sales_order_number,
        rcl.sales_order_line_id,
        rcl.customer_number,
        wf.accounting_segment,
        cal.period_name,
        sum(wf.t_at) as amount
    from waterfall_summary wf
    inner join act on wf.accounting_type_id = act.accounting_type_id
    inner join rcl on wf.root_line_id = rcl.revenue_contract_line_id
    inner join
        rc
        on rcl.revenue_contract_id = rc.revenue_contract_id
        and rcl.book_id = rc.book_id
    inner join
        pob
        on rcl.revenue_contract_performance_obligation_id
        = pob.revenue_contract_performance_obligation_id
    inner join rb on wf.book_id = rb.book_id
    inner join org on wf.security_attribute_value = org.organization_id
    inner join cal on wf.period_id = cal.period_id
    left join zuora_account on rcl.customer_number = zuora_account.account_number
    where
        act.is_waterfall_account = 'Y' and act.is_cost = 'N'
        {{ dbt_utils.group_by(n=13) }}

),
rcl_max_prd as (

    select revenue_contract_line_id, max(period_id) as rcl_max_prd_id
    from waterfall {{ dbt_utils.group_by(n=1) }}

),
rcl_min_prd as (

    select revenue_contract_line_id, min(period_id) as rcl_min_prd_id
    from waterfall {{ dbt_utils.group_by(n=1) }}

),
rc_max_prd as (

    select revenue_contract_id, max(period_id) as rc_max_prd_id
    from waterfall
    group by 1

),
last_waterfall_line as (

    select *
    from waterfall
    qualify
        rank() over (
            partition by revenue_contract_line_id
            order by as_of_period_id desc, period_id desc
        ) = 1

),
records_to_insert as (

    /* 
  Records are inserted based on the last waterfall line available. They will repeat until the last transaction in a revenue contract is fully released.
*/
    select
        cal.period_id as as_of_period_id,
        cal.period_id as period_id,
        last_waterfall_line.book_name,
        last_waterfall_line.organization_name,
        last_waterfall_line.revenue_contract_id,
        last_waterfall_line.revenue_contract_performance_obligation_name,
        last_waterfall_line.revenue_contract_line_id,
        last_waterfall_line.revenue_contract_customer_name,
        last_waterfall_line.sales_order_number,
        last_waterfall_line.sales_order_line_id,
        last_waterfall_line.customer_number,
        last_waterfall_line.accounting_segment,
        -- last_waterfall_line.accounting_type_id,
        last_waterfall_line.period_name,
        0 as amount
    from last_waterfall_line
    cross join cal
    left join
        rcl_max_prd
        on last_waterfall_line.revenue_contract_line_id
        = rcl_max_prd.revenue_contract_line_id
    left join
        rcl_min_prd
        on last_waterfall_line.revenue_contract_line_id
        = rcl_min_prd.revenue_contract_line_id
    left join
        rc_max_prd
        on last_waterfall_line.revenue_contract_id = rc_max_prd.revenue_contract_id
    left join
        waterfall
        on last_waterfall_line.revenue_contract_line_id
        = waterfall.revenue_contract_line_id
        and cal.period_id = waterfall.as_of_period_id
        and cal.period_id = waterfall.period_id
    where
        cal.period_id >= rcl_min_prd.rcl_min_prd_id
        and cal.period_id <= rc_max_prd.rc_max_prd_id
        and waterfall.revenue_contract_line_id is null

),
unioned_waterfall as (

    select * from waterfall UNION ALL select * from records_to_insert

),
previous_revenue_base as (

    select
        revenue_contract_line_id,
        as_of_period_id,
        accounting_segment,
        period_id,
        sum(amount) as amount
    from unioned_waterfall as waterfall
    where as_of_period_id = period_id {{ dbt_utils.group_by(n=4) }}

),
previous_revenue as (

    /*
  To add a column with prior released amounts, this CTE sums the amount released in all periods prior to the current records for each revenue contract line,
  accounting type, accounting segment combination
*/
    select
        previous_revenue_base.revenue_contract_line_id,
        previous_revenue_base.as_of_period_id,
        previous_revenue_base.period_id,
        previous_revenue_base.amount,
        accounting_segment,
        sum(amount) over (
            partition by revenue_contract_line_id, accounting_segment
            order by period_id asc
            rows between unbounded preceding and 1 preceding
        ) as previous_total
    from previous_revenue_base {{ dbt_utils.group_by(n=5) }}

),
waterfall_with_previous_revenue as (

    select
        unioned_waterfall.*, zeroifnull(previous_revenue.previous_total) as prior_total
    from unioned_waterfall
    left join
        previous_revenue
        on unioned_waterfall.revenue_contract_line_id
        = previous_revenue.revenue_contract_line_id
        and unioned_waterfall.as_of_period_id = previous_revenue.as_of_period_id
        and unioned_waterfall.accounting_segment = previous_revenue.accounting_segment

),
final_waterfall_pivot as (

    select
        waterfall_with_previous_revenue.as_of_period_id,
        waterfall_with_previous_revenue.book_name,
        max(org.entity_id) as entity_id,
        waterfall_with_previous_revenue.organization_name,
        waterfall_with_previous_revenue.revenue_contract_customer_name,
        max(rcl.subscription_name) as subscription_name,
        {{ get_keyed_nulls("waterfall_with_previous_revenue.sales_order_line_id") }}
        as sales_order_line_id,
        waterfall_with_previous_revenue.revenue_contract_id,
        max(rcl.rate_plan_name) as rate_plan_name,
        max(rcl.rate_plan_charge_name) as rate_plan_charge_name,
        waterfall_with_previous_revenue.revenue_contract_performance_obligation_name,
        waterfall_with_previous_revenue.accounting_segment,
        max(rcl.subscription_start_date) as subscription_start_date,
        max(rcl.revenue_start_date) as revenue_start_date,
        max(rcl.revenue_end_date) as revenue_end_date,
        max(rcl.product_family) as product_family,
        max(rcl.item_number) as item_number,
        max(zuora_contact_source.country) as country,
        max(rcl.subscription_end_date) as subscription_end_date,
        waterfall_with_previous_revenue.customer_number,
        max(
            rcl.revenue_contract_line_attribute_16
        ) as revenue_contract_line_attribute_16,
        {{
            dbt_utils.pivot(
                "period_name",
                get_column_values_ordered(
                    table=ref("zuora_revenue_calendar_source"),
                    column="period_name",
                    order_by="SUM(period_id)",
                ),
                agg="SUM",
                then_value="amount",
                else_value=0,
            )
        }}
    from waterfall_with_previous_revenue
    left join
        rcl
        on waterfall_with_previous_revenue.revenue_contract_line_id
        = rcl.revenue_contract_line_id
    left join
        org on waterfall_with_previous_revenue.organization_name = org.organization_name
    left join
        zuora_account
        on waterfall_with_previous_revenue.customer_number
        = zuora_account.account_number
    left join
        zuora_contact_source on coalesce(
            zuora_account.sold_to_contact_id, zuora_account.bill_to_contact_id
        ) = zuora_contact_source.contact_id
    group by 1, 2, 4, 5, 7, 8, 11, 12, 20

),
final_waterfall_with_key as (

    select
        {{
            dbt_utils.surrogate_key(
                [
                    "CONCAT(as_of_period_id, sales_order_line_id, revenue_contract_id, accounting_segment)"
                ]
            )
        }}
        as primary_key,
        *
    from final_waterfall_pivot

)

{{
    dbt_audit(
        cte_ref="final_waterfall_with_key",
        created_by="@michellecooper",
        updated_by="@michellecooper",
        created_date="2021-11-08",
        updated_date="2021-11-18",
    )
}}
