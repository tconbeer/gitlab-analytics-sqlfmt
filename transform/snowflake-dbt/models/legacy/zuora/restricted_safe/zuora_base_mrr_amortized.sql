with
    zuora_mrr as (select * from {{ ref("zuora_base_mrr") }}),
    date_table as (select * from {{ ref("date_details") }} where day_of_month = 1),
    amortized_mrr as (

        select
            country,
            account_number,
            subscription_name,
            subscription_name_slugify,
            oldest_subscription_in_cohort,
            lineage,
            rate_plan_name,
            product_category,
            delivery,
            rate_plan_charge_name,
            mrr,
            date_actual as mrr_month,
            sub_start_month,
            sub_end_month,
            effective_start_month,
            effective_end_month,
            effective_start_date,
            effective_end_date,
            cohort_month,
            cohort_quarter,
            unit_of_measure,
            quantity
        from zuora_mrr b
        left join
            date_table d
            on d.date_actual >= b.effective_start_month
            and d.date_actual <= b.effective_end_month


    ),
    final as (

        select
            country,
            account_number,
            subscription_name,
            subscription_name_slugify,
            oldest_subscription_in_cohort,
            lineage,
            rate_plan_name,
            product_category,
            delivery,
            rate_plan_charge_name,
            mrr_month,
            cohort_month,
            cohort_quarter,
            unit_of_measure,
            sum(mrr) as mrr,
            sum(quantity) as quantity
        from amortized_mrr
        where mrr_month is not null {{ dbt_utils.group_by(n=14) }}

    )

select *
from final
