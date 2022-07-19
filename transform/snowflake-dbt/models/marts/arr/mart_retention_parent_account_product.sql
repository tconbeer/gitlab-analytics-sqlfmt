with
    dim_crm_account as (select * from {{ ref("dim_crm_account") }}),
    dim_date as (select * from {{ ref("dim_date") }}),
    dim_product_detail as (select * from {{ ref("dim_product_detail") }}),
    dim_subscription as (select * from {{ ref("dim_subscription") }}),
    fct_mrr as (

        select *
        from {{ ref("fct_mrr") }}
        where subscription_status in ('Active', 'Cancelled')

    ),
    next_renewal_month as (

        select distinct
            merged_accounts.dim_parent_crm_account_id,
            product_tier_name as product_category,
            min(subscription_end_month) over (
                partition by
                    merged_accounts.dim_parent_crm_account_id, product_tier_name
            ) as next_renewal_month_product
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
        inner join
            dim_product_detail
            on dim_product_detail.dim_product_detail_id = fct_mrr.dim_product_detail_id
        where subscription_end_month >= date_trunc('month', current_date)

    ),
    last_renewal_month as (

        select distinct
            merged_accounts.dim_parent_crm_account_id,
            product_tier_name as product_category,
            max(subscription_end_month) over (
                partition by
                    merged_accounts.dim_parent_crm_account_id, product_tier_name
            ) as last_renewal_month_product
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
        inner join
            dim_product_detail
            on dim_product_detail.dim_product_detail_id = fct_mrr.dim_product_detail_id
        where subscription_end_month < date_trunc('month', current_date)

    ),
    parent_account_mrrs as (

        select
            dim_crm_account.dim_parent_crm_account_id,
            dim_product_detail.product_tier_name as product_category,
            dim_product_detail.product_ranking,
            dim_date.date_actual as mrr_month,
            dateadd('year', 1, date_actual) as retention_month,
            next_renewal_month_product,
            last_renewal_month_product,
            sum(zeroifnull(mrr)) as mrr_total,
            sum(zeroifnull(arr)) as arr_total,
            sum(zeroifnull(quantity)) as quantity_total
        from fct_mrr
        inner join dim_date on dim_date.date_id = fct_mrr.dim_date_id
        inner join
            dim_product_detail
            on dim_product_detail.dim_product_detail_id = fct_mrr.dim_product_detail_id
        left join
            dim_crm_account
            on dim_crm_account.dim_crm_account_id = fct_mrr.dim_crm_account_id
        left join
            next_renewal_month
            on next_renewal_month.dim_parent_crm_account_id
            = dim_crm_account.dim_parent_crm_account_id
            and next_renewal_month.product_category
            = dim_product_detail.product_tier_name
        left join
            last_renewal_month
            on last_renewal_month.dim_parent_crm_account_id
            = dim_crm_account.dim_parent_crm_account_id
            and last_renewal_month.product_category
            = dim_product_detail.product_tier_name
        where dim_crm_account.is_jihu_account != 'TRUE' {{ dbt_utils.group_by(n=7) }}

    ),
    retention_subs as (

        select
            current_mrr.dim_parent_crm_account_id,
            current_mrr.product_category,
            current_mrr.product_ranking,
            current_mrr.mrr_month as current_mrr_month,
            current_mrr.retention_month,
            current_mrr.mrr_total as current_mrr,
            future_mrr.mrr_total as future_mrr,
            current_mrr.arr_total as current_arr,
            future_mrr.arr_total as future_arr,
            current_mrr.quantity_total as current_quantity,
            future_mrr.quantity_total as future_quantity,
            current_mrr.last_renewal_month_product,
            current_mrr.next_renewal_month_product,
            -- The type of arr change requires a row_number. Row_number = 1 indicates
            -- new in the macro; however, for retention, new is not a valid option
            -- since retention starts in month 12, well after the First Order
            -- transaction.
            2 as row_number
        from parent_account_mrrs as current_mrr
        left join
            parent_account_mrrs as future_mrr
            on current_mrr.dim_parent_crm_account_id
            = future_mrr.dim_parent_crm_account_id
            and current_mrr.product_category = future_mrr.product_category
            and current_mrr.retention_month = future_mrr.mrr_month

    ),
    final as (

        select
            {{
                dbt_utils.surrogate_key(
                    [
                        "retention_subs.dim_parent_crm_account_id",
                        "product_category",
                        "retention_month",
                    ]
                )
            }} as primary_key,
            retention_subs.dim_parent_crm_account_id,
            dim_crm_account.crm_account_name as parent_crm_account_name,
            product_category,
            product_ranking,
            retention_month,
            dim_date.fiscal_year as retention_fiscal_year,
            dim_date.fiscal_quarter as retention_fiscal_quarter,
            retention_subs.last_renewal_month_product,
            retention_subs.next_renewal_month_product,
            current_mrr as prior_year_mrr,
            coalesce(future_mrr, 0) as net_retention_mrr,
            case
                when net_retention_mrr > 0
                then least(net_retention_mrr, current_mrr)
                else 0
            end as gross_retention_mrr,
            current_arr as prior_year_arr,
            coalesce(future_arr, 0) as net_retention_arr,
            case
                when net_retention_arr > 0
                then least(net_retention_arr, current_arr)
                else 0
            end as gross_retention_arr,
            current_quantity as prior_year_quantity,
            coalesce(future_quantity, 0) as net_retention_quantity,
            {{
                reason_for_quantity_change_seat_change(
                    "net_retention_quantity", "prior_year_quantity"
                )
            }},
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
                    "product_category",
                    "product_category",
                    "net_retention_quantity",
                    "prior_year_quantity",
                    "net_retention_arr",
                    "prior_year_arr",
                    "product_ranking",
                    "product_ranking",
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
            on dim_crm_account.dim_crm_account_id
            = retention_subs.dim_parent_crm_account_id
        where retention_month <= dateadd(month, -1, current_date)

    )

select *
from final
