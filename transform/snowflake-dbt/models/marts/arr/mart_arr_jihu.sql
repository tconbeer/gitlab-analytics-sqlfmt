/* This table needs to be permanent to allow zero cloning at specific timestamps */
{{
    config(
        {
            "materialized": "table",
            "transient": false,
            "schema": "restricted_safe_common_mart_sales",
        }
    )
}}

with
    dim_billing_account as (select * from {{ ref("dim_billing_account") }}),
    dim_crm_account as (select * from {{ ref("dim_crm_account") }}),
    dim_date as (select * from {{ ref("dim_date") }}),
    dim_product_detail as (select * from {{ ref("dim_product_detail") }}),
    dim_subscription as (select * from {{ ref("dim_subscription") }}),
    fct_mrr as (

        select
            dim_date_id,
            dim_subscription_id,
            dim_product_detail_id,
            dim_billing_account_id,
            dim_crm_account_id,
            sum(mrr) as mrr,
            sum(arr) as arr,
            sum(quantity) as quantity,
            array_agg(unit_of_measure) as unit_of_measure
        from {{ ref("fct_mrr") }}
        where
            subscription_status in ('Active', 'Cancelled') {{ dbt_utils.group_by(n=5) }}

    ),
    joined as (

        select
            -- primary_key
            {{
                dbt_utils.surrogate_key(
                    [
                        "fct_mrr.dim_date_id",
                        "dim_subscription.subscription_name",
                        "fct_mrr.dim_product_detail_id",
                    ]
                )
            }} as primary_key,

            -- date info
            dim_date.date_actual as arr_month,
            iff(
                is_first_day_of_last_month_of_fiscal_quarter,
                fiscal_quarter_name_fy,
                null
            ) as fiscal_quarter_name_fy,
            iff(
                is_first_day_of_last_month_of_fiscal_year, fiscal_year, null
            ) as fiscal_year,
            dim_subscription.subscription_start_month as subscription_start_month,
            dim_subscription.subscription_end_month as subscription_end_month,

            -- billing account info
            dim_billing_account.dim_billing_account_id as dim_billing_account_id,
            dim_billing_account.sold_to_country as sold_to_country,
            dim_billing_account.billing_account_name as billing_account_name,
            dim_billing_account.billing_account_number as billing_account_number,
            dim_billing_account.ssp_channel as ssp_channel,
            dim_billing_account.po_required as po_required,

            -- crm account info
            dim_crm_account.dim_crm_account_id as dim_crm_account_id,
            dim_crm_account.crm_account_name as crm_account_name,
            dim_crm_account.dim_parent_crm_account_id as dim_parent_crm_account_id,
            dim_crm_account.parent_crm_account_name as parent_crm_account_name,
            dim_crm_account.parent_crm_account_billing_country
            as parent_crm_account_billing_country,
            dim_crm_account.parent_crm_account_sales_segment
            as parent_crm_account_sales_segment,
            dim_crm_account.parent_crm_account_industry as parent_crm_account_industry,
            dim_crm_account.parent_crm_account_owner_team
            as parent_crm_account_owner_team,
            dim_crm_account.parent_crm_account_sales_territory
            as parent_crm_account_sales_territory,
            dim_crm_account.parent_crm_account_tsp_region
            as parent_crm_account_tsp_region,
            dim_crm_account.parent_crm_account_tsp_sub_region
            as parent_crm_account_tsp_sub_region,
            dim_crm_account.parent_crm_account_tsp_area as parent_crm_account_tsp_area,
            dim_crm_account.parent_crm_account_tsp_account_employees
            as parent_crm_account_tsp_account_employees,
            dim_crm_account.parent_crm_account_tsp_max_family_employees
            as parent_crm_account_tsp_max_family_employees,
            dim_crm_account.parent_crm_account_employee_count_band
            as parent_crm_account_employee_count_band,
            dim_crm_account.crm_account_tsp_region as crm_account_tsp_region,
            dim_crm_account.crm_account_tsp_sub_region as crm_account_tsp_sub_region,
            dim_crm_account.crm_account_tsp_area as crm_account_tsp_area,
            dim_crm_account.health_score as health_score,
            dim_crm_account.health_score_color as health_score_color,
            dim_crm_account.health_number as health_number,
            dim_crm_account.is_jihu_account as is_jihu_account,

            -- subscription info
            dim_subscription.dim_subscription_id as dim_subscription_id,
            dim_subscription.dim_subscription_id_original
            as dim_subscription_id_original,
            dim_subscription.subscription_status as subscription_status,
            dim_subscription.subscription_sales_type as subscription_sales_type,
            dim_subscription.subscription_name as subscription_name,
            dim_subscription.subscription_name_slugify as subscription_name_slugify,
            dim_subscription.oldest_subscription_in_cohort
            as oldest_subscription_in_cohort,
            dim_subscription.subscription_lineage as subscription_lineage,
            dim_subscription.subscription_cohort_month as subscription_cohort_month,
            dim_subscription.subscription_cohort_quarter as subscription_cohort_quarter,
            min(dim_date.date_actual) over (
                partition by dim_billing_account.dim_billing_account_id
            ) as billing_account_cohort_month,
            min(dim_date.first_day_of_fiscal_quarter) over (
                partition by dim_billing_account.dim_billing_account_id
            ) as billing_account_cohort_quarter,
            min(dim_date.date_actual) over (
                partition by dim_crm_account.dim_crm_account_id
            ) as crm_account_cohort_month,
            min(dim_date.first_day_of_fiscal_quarter) over (
                partition by dim_crm_account.dim_crm_account_id
            ) as crm_account_cohort_quarter,
            min(dim_date.date_actual) over (
                partition by dim_crm_account.dim_parent_crm_account_id
            ) as parent_account_cohort_month,
            min(dim_date.first_day_of_fiscal_quarter) over (
                partition by dim_crm_account.dim_parent_crm_account_id
            ) as parent_account_cohort_quarter,
            dim_subscription.auto_renew_native_hist,
            dim_subscription.auto_renew_customerdot_hist,
            dim_subscription.turn_on_cloud_licensing,
            dim_subscription.turn_on_operational_metrics,
            dim_subscription.contract_operational_metrics,
            dim_subscription.contract_auto_renewal,
            dim_subscription.turn_on_auto_renewal,
            dim_subscription.contract_seat_reconciliation,
            dim_subscription.turn_on_seat_reconciliation,

            -- product info
            dim_product_detail.product_tier_name as product_tier_name,
            dim_product_detail.product_delivery_type as product_delivery_type,
            dim_product_detail.service_type as service_type,
            dim_product_detail.product_rate_plan_name as product_rate_plan_name,

            -- MRR values
            -- not needed as all charges in fct_mrr are recurring
            -- fct_mrr.charge_type,
            fct_mrr.unit_of_measure as unit_of_measure,
            fct_mrr.mrr as mrr,
            fct_mrr.arr as arr,
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

    ),
    cohort_diffs as (

        select
            joined.*,
            datediff(
                month, billing_account_cohort_month, arr_month
            ) as months_since_billing_account_cohort_start,
            datediff(
                quarter, billing_account_cohort_quarter, arr_month
            ) as quarters_since_billing_account_cohort_start,
            datediff(
                month, crm_account_cohort_month, arr_month
            ) as months_since_crm_account_cohort_start,
            datediff(
                quarter, crm_account_cohort_quarter, arr_month
            ) as quarters_since_crm_account_cohort_start,
            datediff(
                month, parent_account_cohort_month, arr_month
            ) as months_since_parent_account_cohort_start,
            datediff(
                quarter, parent_account_cohort_quarter, arr_month
            ) as quarters_since_parent_account_cohort_start,
            datediff(
                month, subscription_cohort_month, arr_month
            ) as months_since_subscription_cohort_start,
            datediff(
                quarter, subscription_cohort_quarter, arr_month
            ) as quarters_since_subscription_cohort_start
        from joined

    ),
    parent_arr as (

        select arr_month, dim_parent_crm_account_id, sum(arr) as arr
        from joined {{ dbt_utils.group_by(n=2) }}

    ),
    parent_arr_band_calc as (

        select
            arr_month,
            dim_parent_crm_account_id,
            case
                when arr > 5000 then 'ARR > $5K' when arr <= 5000 then 'ARR <= $5K'
            end as arr_band_calc
        from parent_arr

    ),
    final_table as (

        select cohort_diffs.*, arr_band_calc
        from cohort_diffs
        left join
            parent_arr_band_calc
            on cohort_diffs.arr_month = parent_arr_band_calc.arr_month
            and cohort_diffs.dim_parent_crm_account_id
            = parent_arr_band_calc.dim_parent_crm_account_id

    )

    {{
        dbt_audit(
            cte_ref="final_table",
            created_by="@iweeks",
            updated_by="@iweeks",
            created_date="2021-12-30",
            updated_date="2021-12-30",
        )
    }}
