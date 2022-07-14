{{
    config(
        {
            "materialized": "incremental",
            "unique_key": "primary_key",
            "tags": ["arr_snapshots"],
            "schema": "restricted_safe_common_mart_sales",
        }
    )
}}

with
    dim_billing_account as (

        select * from {{ ref("dim_billing_account_snapshot_bottom_up") }}

    ),
    dim_crm_account as (select * from {{ ref("dim_crm_account") }}),
    dim_crm_account_daily_snapshot as (

        select * from {{ ref("dim_crm_account_daily_snapshot") }}

    ),
    dim_date as (select * from {{ ref("dim_date") }}),
    dim_product_detail as (select * from {{ ref("dim_product_detail") }}),
    dim_subscription as (

        select * from {{ ref("dim_subscription_snapshot_bottom_up") }}

    ),
    fct_mrr_snapshot_bottom_up as (

        select
            mrr_snapshot_id,
            mrr_id,
            snapshot_id,
            dim_date_id,
            dim_subscription_id,
            dim_product_detail_id,
            dim_billing_account_id,
            dim_crm_account_id,
            sum(mrr) as mrr,
            sum(arr) as arr,
            sum(quantity) as quantity,
            array_agg(unit_of_measure) as unit_of_measure
        from {{ ref("fct_mrr_snapshot_bottom_up") }}

        {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        where
            snapshot_id
            > (
                select max(dim_date.date_id)
                from {{ this }}
                inner join dim_date on dim_date.date_actual = snapshot_date
            )

        {% endif %} {{ dbt_utils.group_by(n=8) }}

    ),
    joined as (

        select
            -- keys
            fct_mrr_snapshot_bottom_up.mrr_snapshot_id as primary_key,
            fct_mrr_snapshot_bottom_up.mrr_id,

            -- date info
            snapshot_dates.date_actual as snapshot_date,
            arr_month.date_actual as arr_month,
            iff(
                arr_month.is_first_day_of_last_month_of_fiscal_quarter,
                arr_month.fiscal_quarter_name_fy,
                null
            ) as fiscal_quarter_name_fy,
            iff(
                arr_month.is_first_day_of_last_month_of_fiscal_year,
                arr_month.fiscal_year,
                null
            ) as fiscal_year,
            dim_subscription.term_start_date as term_start_date,
            dim_subscription.term_end_date as term_end_date,
            dim_subscription.subscription_start_month as subscription_start_month,
            dim_subscription.subscription_end_month as subscription_end_month,
            dim_subscription.subscription_start_date as subscription_start_date,
            dim_subscription.subscription_end_date as subscription_end_date,

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
            dim_crm_account.parent_crm_account_lam as parent_crm_account_lam,
            dim_crm_account.parent_crm_account_lam_dev_count
            as parent_crm_account_lam_dev_count,
            dim_crm_account_daily_snapshot.parent_crm_account_lam
            as parent_crm_account_lam_historical,
            dim_crm_account_daily_snapshot.parent_crm_account_lam_dev_count
            as parent_crm_account_lam_dev_count_historical,

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
            min(arr_month.date_actual) OVER (
                partition by
                    dim_billing_account.dim_billing_account_id,
                    snapshot_dates.date_actual
            ) as billing_account_cohort_month,
            min(arr_month.first_day_of_fiscal_quarter) OVER (
                partition by
                    dim_billing_account.dim_billing_account_id,
                    snapshot_dates.date_actual
            ) as billing_account_cohort_quarter,
            min(arr_month.date_actual) OVER (
                partition by
                    dim_crm_account.dim_crm_account_id, snapshot_dates.date_actual
            ) as crm_account_cohort_month,
            min(arr_month.first_day_of_fiscal_quarter) OVER (
                partition by
                    dim_crm_account.dim_crm_account_id, snapshot_dates.date_actual
            ) as crm_account_cohort_quarter,
            min(arr_month.date_actual) OVER (
                partition by
                    dim_crm_account.dim_parent_crm_account_id,
                    snapshot_dates.date_actual
            ) as parent_account_cohort_month,
            min(arr_month.first_day_of_fiscal_quarter) OVER (
                partition by
                    dim_crm_account.dim_parent_crm_account_id,
                    snapshot_dates.date_actual
            ) as parent_account_cohort_quarter,
            dim_subscription.turn_on_cloud_licensing as turn_on_cloud_licensing,
            dim_subscription.turn_on_operational_metrics as turn_on_operational_metrics,
            dim_subscription.contract_operational_metrics
            as contract_operational_metrics,
            dim_subscription.contract_auto_renewal as contract_auto_renewal,
            dim_subscription.turn_on_auto_renewal as turn_on_auto_renewal,
            dim_subscription.contract_seat_reconciliation
            as contract_seat_reconciliation,
            dim_subscription.turn_on_seat_reconciliation as turn_on_seat_reconciliation,

            -- product info
            dim_product_detail.dim_product_detail_id as dim_product_detail_id,
            dim_product_detail.product_tier_name as product_tier_name,
            dim_product_detail.product_delivery_type as product_delivery_type,
            dim_product_detail.service_type as service_type,
            dim_product_detail.product_rate_plan_name as product_rate_plan_name,

            -- charge information
            fct_mrr_snapshot_bottom_up.unit_of_measure as unit_of_measure,
            fct_mrr_snapshot_bottom_up.mrr as mrr,
            fct_mrr_snapshot_bottom_up.arr as arr,
            fct_mrr_snapshot_bottom_up.quantity as quantity
        from fct_mrr_snapshot_bottom_up
        inner join
            dim_subscription
            on dim_subscription.dim_subscription_id
            = fct_mrr_snapshot_bottom_up.dim_subscription_id
            and dim_subscription.snapshot_id = fct_mrr_snapshot_bottom_up.snapshot_id
        inner join
            dim_billing_account
            on dim_billing_account.dim_billing_account_id
            = fct_mrr_snapshot_bottom_up.dim_billing_account_id
            and dim_billing_account.snapshot_id = fct_mrr_snapshot_bottom_up.snapshot_id
        left join
            dim_crm_account_daily_snapshot
            on dim_billing_account.dim_crm_account_id
            = dim_crm_account_daily_snapshot.dim_crm_account_id
            and dim_billing_account.snapshot_id
            = dim_crm_account_daily_snapshot.snapshot_id
        left join
            dim_product_detail
            on dim_product_detail.dim_product_detail_id
            = fct_mrr_snapshot_bottom_up.dim_product_detail_id
        left join
            dim_date as arr_month
            on arr_month.date_id = fct_mrr_snapshot_bottom_up.dim_date_id
        left join
            dim_date as snapshot_dates
            on snapshot_dates.date_id = fct_mrr_snapshot_bottom_up.snapshot_id
        left join
            dim_crm_account
            on dim_billing_account.dim_crm_account_id
            = dim_crm_account.dim_crm_account_id
        where dim_crm_account.is_jihu_account != 'TRUE'

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

        select snapshot_date, arr_month, dim_parent_crm_account_id, sum(arr) as arr
        from joined {{ dbt_utils.group_by(n=3) }}

    ),
    parent_arr_band_calc as (

        select
            snapshot_date,
            arr_month,
            dim_parent_crm_account_id,
            case
                when arr > 5000 then 'ARR > $5K' when arr <= 5000 then 'ARR <= $5K'
            end as arr_band_calc
        from parent_arr

    ),
    final as (

        select cohort_diffs.*, arr_band_calc
        from cohort_diffs
        left join
            parent_arr_band_calc
            on cohort_diffs.snapshot_date = parent_arr_band_calc.snapshot_date
            and cohort_diffs.arr_month = parent_arr_band_calc.arr_month
            and cohort_diffs.dim_parent_crm_account_id
            = parent_arr_band_calc.dim_parent_crm_account_id

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@iweeks",
            updated_by="@jpeguero",
            created_date="2021-07-29",
            updated_date="2022-02-01",
        )
    }}
