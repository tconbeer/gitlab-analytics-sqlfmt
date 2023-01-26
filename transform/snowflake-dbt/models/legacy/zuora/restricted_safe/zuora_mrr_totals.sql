with
    base_mrr as (select * from {{ ref("zuora_base_mrr_amortized") }}),
    service_type as (  -- calculate service_type

        select
            country,
            account_number,
            subscription_name_slugify,
            subscription_name,
            oldest_subscription_in_cohort,
            lineage,
            mrr_month,
            cohort_month as zuora_subscription_cohort_month,
            cohort_quarter as zuora_subscription_cohort_quarter,
            mrr,
            product_category,
            delivery,
            rate_plan_name,
            case
                when lower(rate_plan_name) like '%support%'
                then 'Support Only'
                else 'Full Service'
            end as service_type,
            unit_of_measure,
            quantity
        from base_mrr

    ),
    uniqueified as (  -- one row per sub slug for counting x product_category x mrr_month combo, with first of other values

        select
            {{
                dbt_utils.surrogate_key(
                    [
                        "mrr_month",
                        "subscription_name",
                        "product_category",
                        "unit_of_measure",
                    ]
                )
            }} as primary_key,
            country,
            account_number,
            subscription_name_slugify,
            subscription_name,
            oldest_subscription_in_cohort,
            lineage,
            mrr_month,
            zuora_subscription_cohort_month,
            zuora_subscription_cohort_quarter,
            product_category,
            delivery,
            unit_of_measure,
            service_type,
            array_agg(rate_plan_name) as rate_plan_name,
            sum(quantity) as quantity,
            sum(mrr) as mrr
        from service_type {{ dbt_utils.group_by(n=14) }}

    )

select
    *,  -- calculate new values
    datediff(
        month, zuora_subscription_cohort_month, mrr_month
    ) as months_since_zuora_subscription_cohort_start,
    datediff(
        quarter, zuora_subscription_cohort_quarter, mrr_month
    ) as quarters_since_zuora_subscription_cohort_start
from uniqueified
