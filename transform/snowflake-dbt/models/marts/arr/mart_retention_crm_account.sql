{{ config({"schema": "restricted_safe_common_mart_sales"}) }}

{{
    simple_cte(
        [
            ("dim_date", "dim_date"),
            ("dim_crm_account", "dim_crm_account"),
            ("dim_product_detail", "dim_product_detail"),
            ("dim_subscription", "dim_subscription"),
        ]
    )
}}

,
fct_mrr as (

    select *
    from {{ ref("fct_mrr") }}
    where subscription_status in ('Active', 'Cancelled')

),
next_renewal_month as (

    select distinct
        merged_accounts.dim_crm_account_id,
        min(subscription_end_month) over (
            partition by merged_accounts.dim_crm_account_id
        ) as next_renewal_month
    from fct_mrr
    inner join dim_date on dim_date.date_id = fct_mrr.dim_date_id
    left join
        dim_crm_account as crm_accounts
        on crm_accounts.dim_crm_account_id = fct_mrr.dim_crm_account_id
    inner join
        dim_crm_account as merged_accounts
        on merged_accounts.dim_crm_account_id = coalesce(
            crm_accounts.merged_to_account_id, crm_accounts.dim_crm_account_id
        )
    left join
        dim_subscription
        on dim_subscription.dim_subscription_id = fct_mrr.dim_subscription_id
        and subscription_end_month <= dateadd('year', 1, date_actual)
    where subscription_end_month >= date_trunc('month', current_date)

),
last_renewal_month as (

    select distinct
        merged_accounts.dim_crm_account_id,
        max(subscription_end_month) over (
            partition by merged_accounts.dim_crm_account_id
        ) as last_renewal_month
    from fct_mrr
    inner join dim_date on dim_date.date_id = fct_mrr.dim_date_id
    left join
        dim_crm_account as crm_accounts
        on crm_accounts.dim_crm_account_id = fct_mrr.dim_crm_account_id
    inner join
        dim_crm_account as merged_accounts
        on merged_accounts.dim_crm_account_id = coalesce(
            crm_accounts.merged_to_account_id, crm_accounts.dim_crm_account_id
        )
    left join
        dim_subscription
        on dim_subscription.dim_subscription_id = fct_mrr.dim_subscription_id
        and subscription_end_month <= dateadd('year', 1, date_actual)
    where subscription_end_month < date_trunc('month', current_date)

),
crm_account_mrrs as (

    select
        dim_crm_account.dim_crm_account_id,
        dim_date.date_actual as mrr_month,
        dateadd('year', 1, date_actual) as retention_month,
        next_renewal_month,
        last_renewal_month,
        count(distinct dim_crm_account.dim_crm_account_id) as crm_customer_count,
        sum(zeroifnull(mrr)) as mrr_total,
        sum(zeroifnull(arr)) as arr_total,
        sum(zeroifnull(quantity)) as quantity_total,
        array_agg(product_tier_name) as product_category,
        max(product_ranking) as product_ranking
    from fct_mrr
    inner join
        dim_product_detail
        on dim_product_detail.dim_product_detail_id = fct_mrr.dim_product_detail_id
    inner join dim_date on dim_date.date_id = fct_mrr.dim_date_id
    left join
        dim_crm_account
        on dim_crm_account.dim_crm_account_id = fct_mrr.dim_crm_account_id
    left join
        next_renewal_month
        on next_renewal_month.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    left join
        last_renewal_month
        on last_renewal_month.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    where is_jihu_account = false {{ dbt_utils.group_by(n=5) }}

),
retention_subs as (

    select
        current_mrr.dim_crm_account_id,
        current_mrr.mrr_month as current_mrr_month,
        current_mrr.retention_month,
        current_mrr.mrr_total as current_mrr,
        future_mrr.mrr_total as future_mrr,
        current_mrr.arr_total as current_arr,
        future_mrr.arr_total as future_arr,
        current_mrr.crm_customer_count as current_crm_customer_count,
        future_mrr.crm_customer_count as future_crm_customer_count,
        current_mrr.quantity_total as current_quantity,
        future_mrr.quantity_total as future_quantity,
        current_mrr.product_category as current_product_category,
        future_mrr.product_category as future_product_category,
        current_mrr.product_ranking as current_product_ranking,
        future_mrr.product_ranking as future_product_ranking,
        current_mrr.last_renewal_month,
        current_mrr.next_renewal_month,
        -- The type of arr change requires a row_number. Row_number = 1 indicates new
        -- in the macro; however, for retention, new is not a valid option since
        -- retention starts in month 12, well after the First Order transaction.
        2 as row_number
    from crm_account_mrrs as current_mrr
    left join
        crm_account_mrrs as future_mrr
        on current_mrr.dim_crm_account_id = future_mrr.dim_crm_account_id
        and current_mrr.retention_month = future_mrr.mrr_month

),
final as (

    select
        {{
            dbt_utils.surrogate_key(
                ["retention_subs.dim_crm_account_id", "retention_month"]
            )
        }} as fct_retention_id,
        retention_subs.dim_crm_account_id as dim_crm_account_id,
        dim_crm_account.crm_account_name as crm_account_name,
        retention_month,
        iff(
            is_first_day_of_last_month_of_fiscal_quarter, fiscal_quarter_name_fy, null
        ) as retention_fiscal_year,
        iff(
            is_first_day_of_last_month_of_fiscal_year, fiscal_year, null
        ) as retention_fiscal_quarter,
        retention_subs.last_renewal_month,
        retention_subs.next_renewal_month,
        current_mrr as prior_year_mrr,
        coalesce(future_mrr, 0) as net_retention_mrr,
        case
            when net_retention_mrr > 0 then least(net_retention_mrr, current_mrr) else 0
        end as gross_retention_mrr,
        current_arr as prior_year_arr,
        coalesce(future_arr, 0) as net_retention_arr,
        case
            when net_retention_arr > 0 then least(net_retention_arr, current_arr) else 0
        end as gross_retention_arr,
        current_quantity as prior_year_quantity,
        coalesce(future_quantity, 0) as net_retention_quantity,
        current_crm_customer_count as prior_year_crm_customer_count,
        coalesce(future_crm_customer_count, 0) as net_retention_crm_customer_count,
        {{
            reason_for_quantity_change_seat_change(
                "net_retention_quantity", "prior_year_quantity"
            )
        }},
        future_product_category as net_retention_product_category,
        current_product_category as prior_year_product_category,
        future_product_ranking as net_retention_product_ranking,
        current_product_ranking as prior_year_product_ranking,
        {{ type_of_arr_change("net_retention_arr", "prior_year_arr", "row_number") }},
        {{
            reason_for_arr_change_seat_change(
                "net_retention_quantity",
                "prior_year_quantity",
                "net_retention_arr",
                "prior_year_arr",
            )
        }},
        {{
            reason_for_arr_change_price_change(
                "net_retention_product_category",
                "prior_year_product_category",
                "net_retention_quantity",
                "prior_year_quantity",
                "net_retention_arr",
                "prior_year_arr",
                "net_retention_product_ranking",
                "prior_year_product_ranking",
            )
        }},
        {{
            reason_for_arr_change_tier_change(
                "net_retention_product_ranking",
                "prior_year_product_ranking",
                "net_retention_quantity",
                "prior_year_quantity",
                "net_retention_arr",
                "prior_year_arr",
            )
        }},
        {{
            annual_price_per_seat_change(
                "net_retention_quantity",
                "prior_year_quantity",
                "net_retention_arr",
                "prior_year_arr",
            )
        }}
    from retention_subs
    inner join dim_date on dim_date.date_actual = retention_subs.retention_month
    left join
        dim_crm_account
        on dim_crm_account.dim_crm_account_id = retention_subs.dim_crm_account_id
    where retention_month <= dateadd(month, -1, current_date)

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@ken_aguilar",
        updated_by="@iweeks",
        created_date="2021-10-22",
        updated_date="2022-04-04",
    )
}}
