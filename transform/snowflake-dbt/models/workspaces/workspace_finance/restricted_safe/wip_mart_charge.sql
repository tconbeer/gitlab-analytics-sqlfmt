with
    dim_amendment as (select * from {{ ref("dim_amendment") }}),
    dim_billing_account as (select * from {{ ref("dim_billing_account") }}),
    dim_charge as (select * from {{ ref("dim_charge") }}),
    dim_crm_account as (select * from {{ ref("dim_crm_account") }}),
    dim_product_detail as (select * from {{ ref("dim_product_detail") }}),
    dim_subscription as (select * from {{ ref("dim_subscription") }}),
    fct_charge as (select * from {{ ref("wip_fct_charge") }}),
    mart_charge as (

        select
            -- Surrogate Key
            dim_charge.dim_charge_id,

            -- Natural Key
            dim_charge.subscription_name,
            dim_charge.subscription_version,
            dim_charge.rate_plan_charge_number,
            dim_charge.rate_plan_charge_version,
            dim_charge.rate_plan_charge_segment,

            -- Charge Information
            dim_charge.charge_type as charge_type,
            dim_charge.is_paid_in_full as is_paid_in_full,
            dim_charge.is_last_segment as is_last_segment,
            dim_charge.is_included_in_arr_calc as is_included_in_arr_calc,
            dim_charge.effective_start_date as effective_start_date,
            dim_charge.effective_end_date as effective_end_date,
            dim_charge.effective_start_month as effective_start_month,
            dim_charge.effective_end_month as effective_end_month,
            dim_charge.charge_created_date as charge_created_date,
            dim_charge.charge_updated_date as charge_updated_date,

            -- Subscription Information
            dim_subscription.dim_subscription_id as dim_subscription_id,
            dim_subscription.subscription_start_date as subscription_start_date,
            dim_subscription.subscription_end_date as subscription_end_date,
            dim_subscription.subscription_start_month as subscription_start_month,
            dim_subscription.subscription_end_month as subscription_end_month,
            dim_subscription.subscription_end_fiscal_year
            as subscription_end_fiscal_year,
            dim_subscription.subscription_created_date as subscription_created_date,
            dim_subscription.subscription_updated_date as subscription_updated_date,
            dim_subscription.second_active_renewal_month as second_active_renewal_month,
            dim_subscription.subscription_status as subscription_status,
            dim_subscription.subscription_sales_type as subscription_sales_type,
            dim_subscription.subscription_name_slugify as subscription_name_slugify,
            dim_subscription.oldest_subscription_in_cohort
            as oldest_subscription_in_cohort,
            dim_subscription.subscription_lineage as subscription_lineage,

            -- billing account info
            dim_billing_account.dim_billing_account_id as dim_billing_account_id,
            dim_billing_account.sold_to_country as sold_to_country,
            dim_billing_account.billing_account_name as billing_account_name,
            dim_billing_account.billing_account_number as billing_account_number,

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
            dim_crm_account.crm_account_tsp_region as crm_account_tsp_region,
            dim_crm_account.crm_account_tsp_sub_region as crm_account_tsp_sub_region,
            dim_crm_account.crm_account_tsp_area as crm_account_tsp_area,
            dim_crm_account.health_score as health_score,
            dim_crm_account.health_score_color as health_score_color,
            dim_crm_account.health_number as health_number,

            -- Cohort Information
            dim_subscription.subscription_cohort_month as subscription_cohort_month,
            dim_subscription.subscription_cohort_quarter as subscription_cohort_quarter,
            min(dim_subscription.subscription_cohort_month) over (
                partition by dim_billing_account.dim_billing_account_id
            ) as billing_account_cohort_month,
            min(dim_subscription.subscription_cohort_quarter) over (
                partition by dim_billing_account.dim_billing_account_id
            ) as billing_account_cohort_quarter,
            min(dim_subscription.subscription_cohort_month) over (
                partition by dim_crm_account.dim_crm_account_id
            ) as crm_account_cohort_month,
            min(dim_subscription.subscription_cohort_quarter) over (
                partition by dim_crm_account.dim_crm_account_id
            ) as crm_account_cohort_quarter,
            min(dim_subscription.subscription_cohort_month) over (
                partition by dim_crm_account.dim_parent_crm_account_id
            ) as parent_account_cohort_month,
            min(dim_subscription.subscription_cohort_quarter) over (
                partition by dim_crm_account.dim_parent_crm_account_id
            ) as parent_account_cohort_quarter,

            -- product info
            dim_product_detail.dim_product_detail_id,
            dim_product_detail.product_tier_name as product_tier_name,
            dim_product_detail.product_delivery_type as product_delivery_type,
            dim_product_detail.service_type as service_type,
            dim_product_detail.product_rate_plan_name as product_rate_plan_name,

            -- Amendment Information
            case
                when dim_charge.subscription_version = 1
                then 'NewSubscription'
                else dim_amendment_subscription.amendment_type
            end as subscription_amendment_type,
            dim_amendment_subscription.amendment_name as subscription_amendment_name,
            case
                when dim_charge.subscription_version = 1
                then 'NewSubscription'
                else dim_amendment_charge.amendment_type
            end as charge_amendment_type,

            -- ARR Analysis Framework
            dim_charge.type_of_arr_change,

            -- Additive Fields
            fct_charge.mrr,
            fct_charge.previous_mrr,
            fct_charge.delta_mrr,
            fct_charge.arr,
            fct_charge.previous_arr,
            fct_charge.delta_arr,
            fct_charge.quantity,
            fct_charge.previous_quantity,
            fct_charge.delta_quantity,
            fct_charge.estimated_total_future_billings

        from fct_charge
        inner join dim_charge on fct_charge.dim_charge_id = dim_charge.dim_charge_id
        inner join
            dim_subscription
            on fct_charge.dim_subscription_id = dim_subscription.dim_subscription_id
        inner join
            dim_product_detail
            on fct_charge.dim_product_detail_id
            = dim_product_detail.dim_product_detail_id
        inner join
            dim_billing_account
            on fct_charge.dim_billing_account_id
            = dim_billing_account.dim_billing_account_id
        left join
            dim_crm_account
            on dim_crm_account.dim_crm_account_id
            = dim_billing_account.dim_crm_account_id
        left join
            dim_amendment as dim_amendment_subscription
            on dim_subscription.dim_amendment_id_subscription
            = dim_amendment_subscription.dim_amendment_id
        left join
            dim_amendment as dim_amendment_charge
            on fct_charge.dim_amendment_id_charge
            = dim_amendment_charge.dim_amendment_id
        order by
            dim_crm_account.dim_parent_crm_account_id,
            dim_crm_account.dim_crm_account_id,
            fct_charge.subscription_name,
            fct_charge.subscription_version,
            fct_charge.rate_plan_charge_number,
            fct_charge.rate_plan_charge_version,
            fct_charge.rate_plan_charge_segment

    )

    {{
        dbt_audit(
            cte_ref="mart_charge",
            created_by="@iweeks",
            updated_by="@iweeks",
            created_date="2021-05-10",
            updated_date="2021-05-10",
        )
    }}
