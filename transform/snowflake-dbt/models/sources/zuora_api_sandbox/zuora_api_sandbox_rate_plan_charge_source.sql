-- values to consider renaming:
-- mrr
-- dmrc
-- dtcv
-- tcv
-- uom
with
    source as (select * from {{ source("zuora_api_sandbox", "rate_plan_charge") }}),
    renamed as (

        select
            id as rate_plan_charge_id,
            name as rate_plan_charge_name,
            -- keys
            originalid as original_id,
            rateplanid as rate_plan_id,
            productrateplanchargeid as product_rate_plan_charge_id,
            productrateplanid as product_rate_plan_id,
            productid as product_id,

            -- recognition
            revenuerecognitionrulename as revenue_recognition_rule_name,
            revreccode as revenue_recognition_code,
            revrectriggercondition as revenue_recognition_trigger_condition,

            -- info
            effectivestartdate as effective_start_date,
            effectiveenddate as effective_end_date,
            date_trunc('month', effectivestartdate)::date as effective_start_month,
            date_trunc('month', effectiveenddate)::date as effective_end_month,
            enddatecondition as end_date_condition,

            mrr,
            quantity as quantity,
            tcv,
            uom as unit_of_measure,

            accountid as account_id,
            accountingcode as accounting_code,
            applydiscountto as apply_discount_to,
            billcycleday as bill_cycle_day,
            billcycletype as bill_cycle_type,
            billingperiod as billing_period,
            billingperiodalignment as billing_period_alignment,
            chargedthroughdate as charged_through_date,
            chargemodel as charge_model,
            chargenumber as rate_plan_charge_number,
            chargetype as charge_type,
            description as description,
            discountlevel as discount_level,
            dmrc
            as delta_mrc,  -- delta monthly recurring charge
            dtcv
            as delta_tcv,  -- delta total contract value

            islastsegment as is_last_segment,
            listpricebase as list_price_base,
            -- numberofperiods                                       AS
            -- number_of_periods,
            overagecalculationoption as overage_calculation_option,
            overageunusedunitscreditoption as overage_unused_units_credit_option,
            processedthroughdate as processed_through_date,

            segment as segment,
            specificbillingperiod as specific_billing_period,
            specificenddate as specific_end_date,
            triggerdate as trigger_date,
            triggerevent as trigger_event,
            uptoperiods as up_to_period,
            uptoperiodstype as up_to_periods_type,
            version as version,

            -- ext1, ext2, ext3, ... ext13
            -- metadata
            createdbyid as created_by_id,
            createddate as created_date,
            updatedbyid as updated_by_id,
            updateddate as updated_date,
            deleted as is_deleted

        from source

    )

select *
from renamed
