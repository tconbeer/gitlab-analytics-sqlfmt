{{ config({"schema": "restricted_safe_common_mart_sales"}) }}

{{
    simple_cte(
        [
            (
                "driveload_financial_metrics_program_phase_1_source",
                "driveload_financial_metrics_program_phase_1_source",
            ),
            ("dim_date", "dim_date"),
            ("mart_arr_snapshot_model", "mart_arr_snapshot_model"),
            ("dim_crm_account", "dim_crm_account"),
            ("zuora_account_source", "zuora_account_source"),
        ]
    )
}},
phase_one as (

    select
        driveload_financial_metrics_program_phase_1_source.arr_month,
        driveload_financial_metrics_program_phase_1_source.fiscal_quarter_name_fy,
        driveload_financial_metrics_program_phase_1_source.fiscal_year,
        driveload_financial_metrics_program_phase_1_source.subscription_start_month,
        driveload_financial_metrics_program_phase_1_source.subscription_end_month,
        driveload_financial_metrics_program_phase_1_source.zuora_account_id
        as dim_billing_account_name,
        driveload_financial_metrics_program_phase_1_source.zuora_sold_to_country
        as sold_to_country,
        driveload_financial_metrics_program_phase_1_source.zuora_account_name
        as billing_account_name,
        driveload_financial_metrics_program_phase_1_source.zuora_account_number
        as billing_account_number,
        driveload_financial_metrics_program_phase_1_source.dim_crm_account_id,
        driveload_financial_metrics_program_phase_1_source.dim_parent_crm_account_id,
        driveload_financial_metrics_program_phase_1_source.parent_crm_account_name,
        driveload_financial_metrics_program_phase_1_source.parent_crm_account_billing_country,
        case
            when
                driveload_financial_metrics_program_phase_1_source.parent_crm_account_sales_segment
                is null
            then 'SMB'
            when
                driveload_financial_metrics_program_phase_1_source.parent_crm_account_sales_segment
                = 'Pubsec'
            then 'PubSec'
            else
                driveload_financial_metrics_program_phase_1_source.parent_crm_account_sales_segment
        end as parent_crm_account_sales_segment,
        driveload_financial_metrics_program_phase_1_source.parent_crm_account_industry,
        driveload_financial_metrics_program_phase_1_source.parent_crm_account_owner_team,
        driveload_financial_metrics_program_phase_1_source.parent_crm_account_sales_territory,
        driveload_financial_metrics_program_phase_1_source.subscription_name,
        driveload_financial_metrics_program_phase_1_source.subscription_status,
        driveload_financial_metrics_program_phase_1_source.subscription_sales_type,
        driveload_financial_metrics_program_phase_1_source.product_name,
        driveload_financial_metrics_program_phase_1_source.product_category
        as product_tier_name,
        case
            when driveload_financial_metrics_program_phase_1_source.delivery = 'Others'
            then 'SaaS'
            else driveload_financial_metrics_program_phase_1_source.delivery
        end as product_delivery_type,
        driveload_financial_metrics_program_phase_1_source.service_type,
        driveload_financial_metrics_program_phase_1_source.unit_of_measure,
        driveload_financial_metrics_program_phase_1_source.mrr,
        driveload_financial_metrics_program_phase_1_source.arr,
        driveload_financial_metrics_program_phase_1_source.quantity,
        driveload_financial_metrics_program_phase_1_source.parent_account_cohort_month,
        driveload_financial_metrics_program_phase_1_source.months_since_parent_account_cohort_start,
        driveload_financial_metrics_program_phase_1_source.parent_crm_account_employee_count_band
    from driveload_financial_metrics_program_phase_1_source
    where arr_month <= '2021-06-01'

),
snapshot_dates as (
    -- Use the 8th calendar day to snapshot ARR, Licensed Users, and Customer Count
    -- Metrics
    select distinct first_day_of_month, snapshot_date_fpa from dim_date order by 1 desc

),
parent_cohort_month_snapshot as (

    select dim_parent_crm_account_id, min(arr_month) as parent_account_cohort_month
    from mart_arr_snapshot_model {{ dbt_utils.group_by(n=1) }}

),
snapshot_model as (

    select
        mart_arr_snapshot_model.arr_month,
        mart_arr_snapshot_model.fiscal_quarter_name_fy,
        mart_arr_snapshot_model.fiscal_year,
        mart_arr_snapshot_model.subscription_start_month,
        mart_arr_snapshot_model.subscription_end_month,
        mart_arr_snapshot_model.dim_billing_account_id,
        mart_arr_snapshot_model.sold_to_country,
        mart_arr_snapshot_model.billing_account_name,
        mart_arr_snapshot_model.billing_account_number,
        mart_arr_snapshot_model.dim_crm_account_id,
        mart_arr_snapshot_model.dim_parent_crm_account_id,
        mart_arr_snapshot_model.parent_crm_account_name,
        mart_arr_snapshot_model.parent_crm_account_billing_country,
        case
            when mart_arr_snapshot_model.parent_crm_account_sales_segment is null
            then 'SMB'
            when mart_arr_snapshot_model.parent_crm_account_sales_segment = 'Pubsec'
            then 'PubSec'
            else mart_arr_snapshot_model.parent_crm_account_sales_segment
        end as parent_crm_account_sales_segment,
        mart_arr_snapshot_model.parent_crm_account_industry,
        mart_arr_snapshot_model.parent_crm_account_owner_team,
        mart_arr_snapshot_model.parent_crm_account_sales_territory,
        mart_arr_snapshot_model.subscription_name,
        mart_arr_snapshot_model.subscription_status,
        mart_arr_snapshot_model.subscription_sales_type,
        case
            when mart_arr_snapshot_model.product_tier_name = 'Self-Managed - Ultimate'
            then 'Ultimate'
            when mart_arr_snapshot_model.product_tier_name = 'Self-Managed - Premium'
            then 'Premium'
            when mart_arr_snapshot_model.product_tier_name = 'Self-Managed - Starter'
            then 'Bronze/Starter'
            when mart_arr_snapshot_model.product_tier_name = 'SaaS - Ultimate'
            then 'Ultimate'
            when mart_arr_snapshot_model.product_tier_name = 'SaaS - Premium'
            then 'Premium'
            when mart_arr_snapshot_model.product_tier_name = 'SaaS - Bronze'
            then 'Bronze/Starter'
            else mart_arr_snapshot_model.product_tier_name
        end as product_name,
        mart_arr_snapshot_model.product_tier_name,
        case
            when mart_arr_snapshot_model.product_delivery_type = 'Others'
            then 'SaaS'
            else mart_arr_snapshot_model.product_delivery_type
        end as product_delivery_type,
        mart_arr_snapshot_model.service_type,
        mart_arr_snapshot_model.unit_of_measure,
        mart_arr_snapshot_model.mrr,
        mart_arr_snapshot_model.arr,
        mart_arr_snapshot_model.quantity,
        parent_cohort_month_snapshot.parent_account_cohort_month
        as parent_account_cohort_month,
        datediff(
            month, parent_cohort_month_snapshot.parent_account_cohort_month, arr_month
        ) as months_since_parent_account_cohort_start,
        mart_arr_snapshot_model.parent_crm_account_employee_count_band
    from mart_arr_snapshot_model
    inner join
        snapshot_dates
        on mart_arr_snapshot_model.arr_month = snapshot_dates.first_day_of_month
        and mart_arr_snapshot_model.snapshot_date = snapshot_dates.snapshot_date_fpa
    -- calculate parent cohort month based on correct cohort logic
    left join
        parent_cohort_month_snapshot
        on mart_arr_snapshot_model.dim_parent_crm_account_id
        = parent_cohort_month_snapshot.dim_parent_crm_account_id
    where
        mart_arr_snapshot_model.is_jihu_account != 'TRUE'
        and mart_arr_snapshot_model.arr_month >= '2021-07-01'
    order by 1 desc

),
combined as (

    select *
    from snapshot_model

    union all

    select *
    from phase_one

),
parent_arr as (

    select arr_month, dim_parent_crm_account_id, sum(arr) as arr
    from combined
    group by 1, 2

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
final as (
    -- Snap in arr_band_calc based on correct logic. Some historical in
    -- mart_arr_snapshot_model do not have the arr_band_calc.
    select
        combined.arr_month,
        fiscal_quarter_name_fy,
        fiscal_year,
        subscription_start_month,
        subscription_end_month,
        combined.dim_billing_account_id,
        sold_to_country,
        billing_account_name,
        billing_account_number,
        combined.dim_crm_account_id,
        combined.dim_parent_crm_account_id,
        combined.parent_crm_account_name,
        parent_crm_account_billing_country,
        parent_crm_account_sales_segment,
        parent_crm_account_industry,
        parent_crm_account_owner_team,
        parent_crm_account_sales_territory,
        subscription_name,
        subscription_status,
        subscription_sales_type,
        product_name,
        product_tier_name,
        product_delivery_type,
        service_type,
        unit_of_measure,
        mrr,
        arr,
        quantity,
        parent_account_cohort_month,
        months_since_parent_account_cohort_start,
        coalesce(
            parent_arr_band_calc.arr_band_calc, 'Missing crm_account_id'
        ) as arr_band_calc,
        parent_crm_account_employee_count_band
    from combined
    left join
        parent_arr_band_calc
        on combined.dim_parent_crm_account_id
        = parent_arr_band_calc.dim_parent_crm_account_id
        and combined.arr_month = parent_arr_band_calc.arr_month

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@iweeks",
        updated_by="@iweeks",
        created_date="2021-08-16",
        updated_date="2021-08-16",
    )
}}
