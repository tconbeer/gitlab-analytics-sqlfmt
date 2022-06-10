with
    sfdc_opportunity_xf as (

        select
            to_number(amount, 38, 2) as opportunity_amount,
            close_date::date as close_date,
            invoice_number,
            net_incremental_acv,
            opportunity_id,
            sales_type
        from {{ ref("sfdc_opportunity_xf") }}
        where stage_name = 'Closed Won' and invoice_number is not null

    ),
    zuora_invoice_charges as (

        select
            effective_start_date,
            effective_end_date,
            invoice_amount_without_tax as invoice_amount,
            invoice_date,
            invoice_item_charge_amount as item_amount,
            invoice_number,
            is_last_segment_version,
            mrr,
            delta_tcv,
            {{ product_category("rate_plan_name") }},
            {{ delivery("product_category") }},
            rate_plan_charge_name as charge_name,
            rate_plan_charge_number as charge_number,
            rate_plan_charge_segment as charge_segment,
            segment_version_order,
            subscription_name_slugify
        from {{ ref("zuora_invoice_charges") }}

    ),
    filtered_charges as (

        select
            zuora_invoice_charges.*,
            iff(
                row_number() over (
                    partition by
                        subscription_name_slugify,
                        invoice_number,
                        charge_number,
                        charge_segment
                    order by segment_version_order desc
                ) = 1,
                true,
                false
            ) as is_last_invoice_segment_version
        from zuora_invoice_charges
        where effective_end_date > invoice_date and mrr > 0
        qualify is_last_invoice_segment_version

    ),
    true_mrr_periods as (

        select
            charge_number,
            charge_segment,
            effective_start_date as true_effective_start_date,
            effective_end_date as true_effective_end_date
        from zuora_invoice_charges
        where is_last_segment_version

    ),
    aggregate_subscription as (

        select
            subscription_name_slugify,
            invoice_number,
            invoice_date,
            count(distinct subscription_name_slugify) over (
                partition by invoice_number
            ) as invoiced_subscriptions,
            to_number(max(invoice_amount), 38, 2) as invoice_amount,
            to_number(sum(item_amount), 38, 2) as subscription_amount,
            to_number(sum(delta_tcv), 38, 2) as delta_tcv,
            to_number(
                sum(iff(charge_name = '1,000 CI Minutes', item_amount, 0)), 38, 2
            ) as ci_minutes_amount,
            to_number(
                sum(iff(charge_name = 'Trueup', item_amount, 0)), 38, 2
            ) as trueup_amount
        from zuora_invoice_charges
        group by 1, 2, 3

    ),
    charge_join as (
        -- first join to opportunities based on invoiced subscription charge amount =
        -- opportunity TCV
        select
            aggregate_subscription.*,
            close_date,
            net_incremental_acv,
            opportunity_amount,
            opportunity_id,
            sales_type
        from aggregate_subscription
        left join
            sfdc_opportunity_xf
            on aggregate_subscription.invoice_number
            = sfdc_opportunity_xf.invoice_number
            and aggregate_subscription.subscription_amount
            = sfdc_opportunity_xf.opportunity_amount

    ),
    tcv_join as (
        -- next join to opportunities based on change in TCV to subscription =
        -- opportunity TCV
        select
            subscription_name_slugify,
            charge_join.invoice_number,
            invoice_date,
            invoiced_subscriptions,
            invoice_amount,
            subscription_amount,
            delta_tcv,
            ci_minutes_amount,
            trueup_amount,
            coalesce(
                charge_join.close_date, sfdc_opportunity_xf.close_date
            ) as close_date,
            coalesce(
                charge_join.net_incremental_acv, sfdc_opportunity_xf.net_incremental_acv
            ) as net_incremental_acv,
            coalesce(
                charge_join.opportunity_amount, sfdc_opportunity_xf.opportunity_amount
            ) as opportunity_amount,
            coalesce(
                charge_join.opportunity_id, sfdc_opportunity_xf.opportunity_id
            ) as opportunity_id,
            coalesce(
                charge_join.sales_type, sfdc_opportunity_xf.sales_type
            ) as sales_type
        from charge_join
        left join
            sfdc_opportunity_xf
            on charge_join.invoice_number = sfdc_opportunity_xf.invoice_number
            and charge_join.delta_tcv = sfdc_opportunity_xf.opportunity_amount
            and charge_join.opportunity_id is null

    ),
    final as (

        select
            -- keys
            tcv_join.subscription_name_slugify,
            tcv_join.invoice_number,
            filtered_charges.charge_number,
            tcv_join.opportunity_id,

            -- dates
            tcv_join.invoice_date,
            filtered_charges.effective_start_date as booked_effective_start_date,
            filtered_charges.effective_end_date as booked_effective_end_date,
            true_mrr_periods.true_effective_start_date,
            true_mrr_periods.true_effective_end_date,
            tcv_join.close_date,

            -- invoice info
            tcv_join.invoiced_subscriptions,
            tcv_join.invoice_amount,
            tcv_join.subscription_amount,
            tcv_join.delta_tcv,
            tcv_join.ci_minutes_amount,
            tcv_join.trueup_amount,
            filtered_charges.item_amount,
            tcv_join.opportunity_amount,

            -- charge info
            filtered_charges.mrr,
            tcv_join.net_incremental_acv,

            -- metadata
            filtered_charges.charge_name,
            filtered_charges.charge_segment,
            filtered_charges.delivery,
            filtered_charges.product_category,
            tcv_join.sales_type,
            max(is_last_segment_version) as is_last_segment_version
        from tcv_join
        inner join
            filtered_charges
            on tcv_join.subscription_name_slugify
            = filtered_charges.subscription_name_slugify
            and tcv_join.invoice_number = filtered_charges.invoice_number
        inner join
            true_mrr_periods
            on filtered_charges.charge_number = true_mrr_periods.charge_number
            and filtered_charges.charge_segment = true_mrr_periods.charge_segment
            {{ dbt_utils.group_by(n=25) }}

    )

select *
from final
