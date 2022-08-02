with
    raw_mrr_totals_levelled as (

        select * from {{ ref("mart_arr") }} where product_tier_name != 'Trueup'

    ),
    mrr_totals_levelled as (

        select
            subscription_name,
            subscription_name_slugify,
            dim_crm_account_id as sfdc_account_id,
            oldest_subscription_in_cohort as oldest_subscription_in_cohort,
            subscription_lineage as lineage,
            arr_month as mrr_month,
            subscription_cohort_month as zuora_subscription_cohort_month,
            subscription_cohort_quarter as zuora_subscription_cohort_quarter,
            months_since_subscription_cohort_start
            as months_since_zuora_subscription_cohort_start,
            quarters_since_subscription_cohort_start
            as quarters_since_zuora_subscription_cohort_start,

            array_agg(
                distinct product_tier_name) within group(order by product_tier_name asc
            ) as original_product_category,
            array_agg(
                distinct product_delivery_type
            ) within group(order by product_delivery_type asc
            ) as original_delivery,
            array_agg(
                distinct unit_of_measure) within group(order by unit_of_measure asc
            ) as original_unit_of_measure,

            max(  -- Need to account for the 'other' categories
                decode(
                    product_tier_name,
                    'Bronze',
                    1,
                    'Silver',
                    2,
                    'Gold',
                    3,

                    'Starter',
                    1,
                    'Premium',
                    2,
                    'Ultimate',
                    3,

                    0
                )
            ) as original_product_ranking,
            sum(quantity) as original_quantity,
            sum(mrr) as original_mrr
        from raw_mrr_totals_levelled {{ dbt_utils.group_by(n=10) }}

    -- get all the subscription + their lineage + the month we're looking for MRR for
    -- (12 month in the future)
    ),
    list as (

        select
            subscription_name_slugify as original_sub,
            c.value::varchar as subscriptions_in_lineage,
            mrr_month as original_mrr_month,
            dateadd('year', 1, mrr_month) as retention_month
        from
            mrr_totals_levelled,
            lateral flatten(input => split(lineage, ',')) c
            {{ dbt_utils.group_by(n=4) }}

    -- find which of those subscriptions are real and group them by their sub you're
    -- comparing to.
    ),
    retention_subs as (

        select
            list.original_sub,
            list.retention_month,
            list.original_mrr_month,
            mrr_totals_levelled.original_product_category as retention_product_category,
            mrr_totals_levelled.original_delivery as retention_delivery,
            mrr_totals_levelled.original_quantity as retention_quantity,
            mrr_totals_levelled.original_unit_of_measure as retention_unit_of_measure,
            mrr_totals_levelled.original_product_ranking as retention_product_ranking,
            coalesce(sum(mrr_totals_levelled.original_mrr), 0) as retention_mrr
        from list
        inner join
            mrr_totals_levelled
            on retention_month = mrr_month
            and subscriptions_in_lineage = subscription_name_slugify
            {{ dbt_utils.group_by(n=8) }}

    ),
    expansion as (

        select
            mrr_totals_levelled.*,
            retention_subs.*,
            coalesce(retention_subs.retention_mrr, 0) as net_retention_mrr,
            {{ retention_type("original_mrr", "net_retention_mrr") }},
            {{
                retention_reason(
                    "original_mrr",
                    "original_product_category",
                    "original_product_ranking",
                    "original_quantity",
                    "net_retention_mrr",
                    "retention_product_category",
                    "retention_product_ranking",
                    "retention_quantity",
                )
            }},
            {{
                plan_change(
                    "original_product_ranking",
                    "original_mrr",
                    "retention_product_ranking",
                    "net_retention_mrr",
                )
            }},
            {{
                seat_change(
                    "original_quantity",
                    "original_unit_of_measure",
                    "original_mrr",
                    "retention_quantity",
                    "retention_unit_of_measure",
                    "net_retention_mrr",
                )
            }},
            {{
                monthly_price_per_seat_change(
                    "original_mrr",
                    "original_quantity",
                    "original_unit_of_measure",
                    "net_retention_mrr",
                    "retention_quantity",
                    "retention_unit_of_measure",
                )
            }}

        from mrr_totals_levelled
        left join
            retention_subs
            on subscription_name_slugify = original_sub
            and retention_subs.original_mrr_month = mrr_totals_levelled.mrr_month
        where retention_mrr > original_mrr

    ),
    churn as (

        select
            mrr_totals_levelled.*,
            retention_subs.*,
            coalesce(retention_subs.retention_mrr, 0) as net_retention_mrr,
            {{ retention_type("original_mrr", "net_retention_mrr") }},
            {{
                retention_reason(
                    "original_mrr",
                    "original_product_category",
                    "original_product_ranking",
                    "original_quantity",
                    "net_retention_mrr",
                    "retention_product_category",
                    "retention_product_ranking",
                    "retention_quantity",
                )
            }},
            {{
                plan_change(
                    "original_product_ranking",
                    "original_mrr",
                    "retention_product_ranking",
                    "net_retention_mrr",
                )
            }},
            {{
                seat_change(
                    "original_quantity",
                    "original_unit_of_measure",
                    "original_mrr",
                    "retention_quantity",
                    "retention_unit_of_measure",
                    "net_retention_mrr",
                )
            }},
            {{
                monthly_price_per_seat_change(
                    "original_mrr",
                    "original_quantity",
                    "original_unit_of_measure",
                    "net_retention_mrr",
                    "retention_quantity",
                    "retention_unit_of_measure",
                )
            }}

        from mrr_totals_levelled
        left join
            retention_subs
            on subscription_name_slugify = original_sub
            and retention_subs.original_mrr_month = mrr_totals_levelled.mrr_month
        where net_retention_mrr < original_mrr

    ),
    joined as (

        select
            subscription_name as zuora_subscription_name,
            oldest_subscription_in_cohort as zuora_subscription_id,
            -- THIS IS THE RETENTION MONTH, NOT THE MRR MONTH!!
            dateadd('year', 1, mrr_month) as retention_month,
            retention_type,
            retention_reason,
            plan_change,
            seat_change,
            monthly_price_per_seat_change,
            original_product_category,
            retention_product_category,
            original_delivery,
            retention_delivery,
            original_quantity,
            retention_quantity,
            original_unit_of_measure,
            retention_unit_of_measure,
            original_mrr,
            net_retention_mrr as retention_mrr
        from expansion

        union all

        select
            subscription_name as zuora_subscription_name,
            oldest_subscription_in_cohort as zuora_subscription_id,
            -- THIS IS THE RETENTION MONTH, NOT THE MRR MONTH!!
            dateadd('year', 1, mrr_month) as retention_month,
            retention_type,
            retention_reason,
            plan_change,
            seat_change,
            monthly_price_per_seat_change,
            original_product_category,
            retention_product_category,
            original_delivery,
            retention_delivery,
            original_quantity,
            retention_quantity,
            original_unit_of_measure,
            retention_unit_of_measure,
            original_mrr,
            net_retention_mrr as retention_mrr
        from churn

    )

select
    joined.*,
    rank() over (
        partition by zuora_subscription_id, retention_type order by retention_month asc
    ) as rank_retention_type_month
from joined
where retention_month <= dateadd(month, -1, current_date)
