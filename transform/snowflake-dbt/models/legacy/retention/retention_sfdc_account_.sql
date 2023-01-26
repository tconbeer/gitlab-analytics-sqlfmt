with
    mrr_totals_levelled as (select * from {{ ref("mart_arr") }}),
    current_arr_segmentation_all_levels as (

        select *
        from {{ ref("current_arr_segmentation_all_levels") }}
        where level_ = 'sfdc_account_id'

    ),
    list as (  -- get all the subscription + their lineage + the month we're looking for MRR for (12 month in the future)

        select
            dim_crm_account_id as sfdc_account_id,
            arr_month as original_mrr_month,
            dateadd('year', 1, arr_month) as retention_month,
            sum(mrr) as mrr
        from mrr_totals_levelled
        group by 1, 2, 3

    ),
    retention_subs as (  -- find which of those subscriptions are real and group them by their sub you're comparing to.

        select
            list.sfdc_account_id,
            list.retention_month,
            list.original_mrr_month,
            sum(list.mrr) as original_mrr,
            sum(future.mrr) as retention_mrr
        from list
        left join
            list as future
            on list.retention_month = future.original_mrr_month
            and list.sfdc_account_id = future.sfdc_account_id
        group by 1, 2, 3

    ),
    finals as (

        select
            sfdc_account_id,
            retention_mrr,
            coalesce(retention_mrr, 0) as net_retention_mrr,
            case
                when net_retention_mrr > 0
                then least(net_retention_mrr, original_mrr)
                else 0
            end as gross_retention_mrr,
            retention_month,
            original_mrr_month,
            original_mrr
        from retention_subs

    ),
    joined as (

        select
            finals.sfdc_account_id,
            finals.sfdc_account_id as salesforce_account_id,
            crm_account_name as sfdc_account_name,
            dateadd('year', 1, finals.original_mrr_month) as retention_month,  -- THIS IS THE RETENTION MONTH, NOT THE MRR MONTH!!
            original_mrr,
            net_retention_mrr,
            gross_retention_mrr,
            crm_account_cohort_month as sfdc_account_cohort_month,
            crm_account_cohort_quarter as sfdc_account_cohort_quarter,
            datediff(
                month, crm_account_cohort_month, original_mrr_month
            ) as months_since_sfdc_account_cohort_start,
            datediff(
                quarter, crm_account_cohort_quarter, original_mrr_month
            ) as quarters_since_sfdc_account_cohort_start,
            {{ churn_type("original_mrr", "net_retention_mrr") }}
        from finals
        left join
            mrr_totals_levelled
            on finals.sfdc_account_id = mrr_totals_levelled.dim_crm_account_id
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12

    )

select joined.*, current_arr_segmentation_all_levels.arr_segmentation
from joined
left join
    current_arr_segmentation_all_levels
    on joined.sfdc_account_id = current_arr_segmentation_all_levels.id
where
    retention_month <= dateadd(month, -1, current_date)

    --
    
