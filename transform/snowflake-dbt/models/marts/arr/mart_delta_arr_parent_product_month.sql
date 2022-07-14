with
    dim_billing_account as (select * from {{ ref("dim_billing_account") }}),
    dim_crm_account as (select * from {{ ref("dim_crm_account") }}),
    dim_date as (select * from {{ ref("dim_date") }}),
    dim_product_detail as (select * from {{ ref("dim_product_detail") }}),
    dim_subscription as (select * from {{ ref("dim_subscription") }}),
    fct_mrr as (

        select *
        from {{ ref("fct_mrr") }}
        where subscription_status in ('Active', 'Cancelled')

    ),
    mart_arr as (

        select
            dim_date.date_actual as arr_month,
            iff(
                is_first_day_of_last_month_of_fiscal_quarter,
                fiscal_quarter_name_fy,
                null
            ) as fiscal_quarter_name_fy,
            iff(
                is_first_day_of_last_month_of_fiscal_year, fiscal_year, null
            ) as fiscal_year,
            dim_crm_account.parent_crm_account_name as parent_crm_account_name,
            dim_crm_account.dim_parent_crm_account_id as dim_parent_crm_account_id,
            dim_product_detail.product_tier_name as product_tier_name,
            dim_product_detail.product_delivery_type as product_delivery_type,
            dim_product_detail.product_ranking as product_ranking,
            fct_mrr.mrr as mrr,
            fct_mrr.quantity as quantity
        from fct_mrr
        inner join
            dim_subscription
            on dim_subscription.dim_subscription_id = fct_mrr.dim_subscription_id
        inner join
            dim_product_detail
            on dim_product_detail.dim_product_detail_id = fct_mrr.dim_product_detail_id
        inner join
            dim_billing_account
            on dim_billing_account.dim_billing_account_id
            = fct_mrr.dim_billing_account_id
        inner join dim_date on dim_date.date_id = fct_mrr.dim_date_id
        left join
            dim_crm_account
            on dim_billing_account.dim_crm_account_id
            = dim_crm_account.dim_crm_account_id
        where dim_crm_account.is_jihu_account != 'TRUE'

    ),
    max_min_month as (

        select
            parent_crm_account_name,
            dim_parent_crm_account_id,
            product_tier_name,
            product_delivery_type,
            product_ranking,
            min(arr_month) as date_month_start,
            -- add 1 month to generate churn month
            dateadd('month', 1, max(arr_month)) as date_month_end
        from mart_arr {{ dbt_utils.group_by(n=5) }}

    ),
    base as (

        select
            parent_crm_account_name,
            dim_parent_crm_account_id,
            product_tier_name,
            product_delivery_type,
            product_ranking,
            dim_date.date_actual as arr_month,
            dim_date.fiscal_quarter_name_fy,
            dim_date.fiscal_year
        from max_min_month
        inner join
            dim_date
            -- all months after start date
            on dim_date.date_actual >= max_min_month.date_month_start
            -- up to and including end date
            and dim_date.date_actual <= max_min_month.date_month_end
            and day_of_month = 1

    ),
    monthly_arr_parent_level as (

        select
            base.arr_month,
            base.parent_crm_account_name,
            base.dim_parent_crm_account_id,
            base.product_tier_name as product_tier_name,
            base.product_delivery_type as product_delivery_type,
            base.product_ranking as product_ranking,
            sum(zeroifnull(quantity)) as quantity,
            sum(zeroifnull(mrr) * 12) as arr
        from base
        left join
            mart_arr
            on base.arr_month = mart_arr.arr_month
            and base.dim_parent_crm_account_id = mart_arr.dim_parent_crm_account_id
            and base.product_tier_name = mart_arr.product_tier_name
            {{ dbt_utils.group_by(n=6) }}

    ),
    prior_month as (

        select
            monthly_arr_parent_level.*,
            coalesce(
                lag(quantity) OVER (
                    partition by dim_parent_crm_account_id, product_tier_name
                    order by arr_month
                ),
                0
            ) as previous_quantity,
            coalesce(
                lag(arr) OVER (
                    partition by dim_parent_crm_account_id, product_tier_name
                    order by arr_month
                ),
                0
            ) as previous_arr,
            row_number() OVER (
                partition by dim_parent_crm_account_id, product_tier_name
                order by arr_month
            ) as row_number
        from monthly_arr_parent_level

    ),
    type_of_arr_change as (

        select
            prior_month.*, {{ type_of_arr_change("arr", "previous_arr", "row_number") }}
        from prior_month

    ),
    reason_for_arr_change_beg as (

        select
            arr_month,
            dim_parent_crm_account_id,
            product_tier_name,
            previous_arr as beg_arr,
            previous_quantity as beg_quantity
        from type_of_arr_change

    ),
    reason_for_arr_change_seat_change as (

        select
            arr_month,
            dim_parent_crm_account_id,
            product_tier_name,
            {{
                reason_for_arr_change_seat_change(
                    "quantity", "previous_quantity", "arr", "previous_arr"
                )
            }},
            {{ reason_for_quantity_change_seat_change("quantity", "previous_quantity") }}
        from type_of_arr_change

    ),
    reason_for_arr_change_price_change as (

        select
            arr_month,
            dim_parent_crm_account_id,
            product_tier_name,
            {{
                reason_for_arr_change_price_change(
                    "product_tier_name",
                    "product_tier_name",
                    "quantity",
                    "previous_quantity",
                    "arr",
                    "previous_arr",
                    "product_ranking",
                    " product_ranking",
                )
            }}
        from type_of_arr_change

    ),
    reason_for_arr_change_end as (

        select
            arr_month,
            dim_parent_crm_account_id,
            product_tier_name,
            arr as end_arr,
            quantity as end_quantity
        from type_of_arr_change

    ),
    annual_price_per_seat_change as (

        select
            arr_month,
            dim_parent_crm_account_id,
            product_tier_name,
            {{
                annual_price_per_seat_change(
                    "quantity", "previous_quantity", "arr", "previous_arr"
                )
            }}
        from type_of_arr_change

    ),
    combined as (

        select
            {{
                dbt_utils.surrogate_key(
                    [
                        "type_of_arr_change.arr_month",
                        "type_of_arr_change.dim_parent_crm_account_id",
                        "type_of_arr_change.product_tier_name",
                    ]
                )
            }} as primary_key,
            type_of_arr_change.arr_month,
            type_of_arr_change.parent_crm_account_name,
            type_of_arr_change.dim_parent_crm_account_id,
            type_of_arr_change.product_tier_name,
            type_of_arr_change.product_delivery_type,
            type_of_arr_change.product_ranking,
            type_of_arr_change.type_of_arr_change,
            reason_for_arr_change_beg.beg_arr,
            reason_for_arr_change_beg.beg_quantity,
            reason_for_arr_change_seat_change.seat_change_arr,
            reason_for_arr_change_seat_change.seat_change_quantity,
            reason_for_arr_change_price_change.price_change_arr,
            reason_for_arr_change_end.end_arr,
            reason_for_arr_change_end.end_quantity,
            annual_price_per_seat_change.annual_price_per_seat_change
        from type_of_arr_change
        left join
            reason_for_arr_change_beg
            on type_of_arr_change.dim_parent_crm_account_id
            = reason_for_arr_change_beg.dim_parent_crm_account_id
            and type_of_arr_change.arr_month = reason_for_arr_change_beg.arr_month
            and type_of_arr_change.product_tier_name
            = reason_for_arr_change_beg.product_tier_name
        left join
            reason_for_arr_change_seat_change
            on type_of_arr_change.dim_parent_crm_account_id
            = reason_for_arr_change_seat_change.dim_parent_crm_account_id
            and type_of_arr_change.arr_month
            = reason_for_arr_change_seat_change.arr_month
            and type_of_arr_change.product_tier_name
            = reason_for_arr_change_seat_change.product_tier_name
        left join
            reason_for_arr_change_price_change
            on type_of_arr_change.dim_parent_crm_account_id
            = reason_for_arr_change_price_change.dim_parent_crm_account_id
            and type_of_arr_change.arr_month
            = reason_for_arr_change_price_change.arr_month
            and type_of_arr_change.product_tier_name
            = reason_for_arr_change_price_change.product_tier_name
        left join
            reason_for_arr_change_end
            on type_of_arr_change.dim_parent_crm_account_id
            = reason_for_arr_change_end.dim_parent_crm_account_id
            and type_of_arr_change.arr_month = reason_for_arr_change_end.arr_month
            and type_of_arr_change.product_tier_name
            = reason_for_arr_change_end.product_tier_name
        left join
            annual_price_per_seat_change
            on type_of_arr_change.dim_parent_crm_account_id
            = annual_price_per_seat_change.dim_parent_crm_account_id
            and type_of_arr_change.arr_month = annual_price_per_seat_change.arr_month
            and type_of_arr_change.product_tier_name
            = annual_price_per_seat_change.product_tier_name

    )

select *
from combined
