{{
    config(
        {
            "alias": "fct_mrr",
            "post-hook": '{{ apply_dynamic_data_masking(columns = [{"arr":"float"},{"dim_charge_id":"string"},{"dim_product_detail_id":"string"},{"created_by":"string"},{"dim_billing_account_id":"string"},{"dim_crm_account_id":"string"},{"dim_subscription_id":"string"},{"mrr":"float"},{"mrr_id":"string"},{"updated_by":"string"}]) }}',
        }
    )
}}

/* grain: one record per rate_plan_charge per month */
{{ simple_cte([("dim_date", "dim_date"), ("prep_charge", "prep_charge")]) }}

,
mrr as (

    select
        {{ dbt_utils.surrogate_key(["dim_date.date_id", "prep_charge.dim_charge_id"]) }}
        as mrr_id,
        dim_date.date_id as dim_date_id,
        prep_charge.dim_charge_id,
        prep_charge.dim_product_detail_id,
        prep_charge.dim_subscription_id,
        prep_charge.dim_billing_account_id,
        prep_charge.dim_crm_account_id,
        prep_charge.subscription_status,
        sum(prep_charge.mrr) as mrr,
        sum(prep_charge.arr) as arr,
        sum(prep_charge.quantity) as quantity,
        array_agg(prep_charge.unit_of_measure) as unit_of_measure
    from prep_charge
    inner join
        dim_date on prep_charge.effective_start_month <= dim_date.date_actual and (
            prep_charge.effective_end_month > dim_date.date_actual
            or prep_charge.effective_end_month is null
        ) and dim_date.day_of_month = 1
    where
        /* This excludes Education customers (charge name EDU or OSS) with free subscriptions.
         Pull in seats from Paid EDU Plans with no ARR */
        subscription_status not in ('Draft') and charge_type = 'Recurring' and (
            mrr != 0 or lower(prep_charge.rate_plan_charge_name) = 'max enrollment'
        )
        {{ dbt_utils.group_by(n=8) }}
)

{{
    dbt_audit(
        cte_ref="mrr",
        created_by="@msendal",
        updated_by="@iweeks",
        created_date="2020-09-10",
        updated_date="2022-04-04",
    )
}}
