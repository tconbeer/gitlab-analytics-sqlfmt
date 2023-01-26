with
    raw_mrr_totals_levelled as (select * from {{ ref("mart_arr") }}),
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
            sum(mrr) as mrr
        from raw_mrr_totals_levelled {{ dbt_utils.group_by(n=10) }}

    ),
    current_arr_segmentation_all_levels as (

        select *
        from {{ ref("current_arr_segmentation_all_levels") }}
        where level_ = 'zuora_subscription_id'

    ),
    mapping as (

        select subscription_name, sfdc_account_id
        from mrr_totals_levelled {{ dbt_utils.group_by(n=2) }}

    ),
    -- get all the subscription + their lineage + the month we're looking for MRR for
    -- (12 month in the future)
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

    ),
    -- find which of those subscriptions are real and group them by their sub you're
    -- comparing to.
    retention_subs as (

        select
            original_sub, retention_month, original_mrr_month, sum(mrr) as retention_mrr
        from list
        inner join
            mrr_totals_levelled as subs
            on retention_month = mrr_month
            and subscriptions_in_lineage = subscription_name_slugify
            {{ dbt_utils.group_by(n=3) }}

    ),
    finals as (

        select
            coalesce(retention_subs.retention_mrr, 0) as net_retention_mrr,
            case
                when net_retention_mrr > 0 then least(net_retention_mrr, mrr) else 0
            end as gross_retention_mrr,
            retention_month,
            mrr_totals_levelled.*
        from mrr_totals_levelled
        left join
            retention_subs
            on subscription_name_slugify = original_sub
            and retention_subs.original_mrr_month = mrr_totals_levelled.mrr_month

    ),
    joined as (

        select
            finals.subscription_name as zuora_subscription_name,
            finals.oldest_subscription_in_cohort as zuora_subscription_id,
            mapping.sfdc_account_id as salesforce_account_id,
            -- THIS IS THE RETENTION MONTH, NOT THE MRR MONTH!!
            dateadd('year', 1, finals.mrr_month) as retention_month,
            finals.mrr as original_mrr,
            finals.net_retention_mrr,
            finals.gross_retention_mrr,
            finals.zuora_subscription_cohort_month,
            finals.zuora_subscription_cohort_quarter,
            finals.months_since_zuora_subscription_cohort_start,
            finals.quarters_since_zuora_subscription_cohort_start,
            {{ churn_type("original_mrr", "net_retention_mrr") }}
        from finals
        left join mapping on mapping.subscription_name = finals.subscription_name

    )

select joined.*, current_arr_segmentation_all_levels.arr_segmentation
from joined
left join
    current_arr_segmentation_all_levels
    on joined.zuora_subscription_id = current_arr_segmentation_all_levels.id
where retention_month <= dateadd(month, -1, current_date)
