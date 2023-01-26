-- values to consider renaming:
-- mrr
-- dmrc
-- dtcv
-- tcv
-- uom
with
    source as (select * from {{ source("zuora_central_sandbox", "rate_plan_charge") }}),
    renamed as (

        select
            id as rate_plan_charge_id,
            name as rate_plan_charge_name,
            -- keys
            original_id as original_id,
            rate_plan_id as rate_plan_id,
            product_rate_plan_charge_id as product_rate_plan_charge_id,
            product_rate_plan_id as product_rate_plan_id,
            product_id as product_id,

            -- recognition
            revenue_recognition_rule_name as revenue_recognition_rule_name,
            rev_rec_code as revenue_recognition_code,
            rev_rec_trigger_condition as revenue_recognition_trigger_condition,

            -- info
            effective_start_date as effective_start_date,
            effective_end_date as effective_end_date,
            date_trunc('month', effective_start_date)::date as effective_start_month,
            date_trunc('month', effective_end_date)::date as effective_end_month,
            end_date_condition as end_date_condition,

            mrr as mrr,
            quantity as quantity,
            tcv as tcv,
            uom as unit_of_measure,

            account_id as account_id,
            accounting_code as accounting_code,
            apply_discount_to as apply_discount_to,
            bill_cycle_day as bill_cycle_day,
            bill_cycle_type as bill_cycle_type,
            billing_period as billing_period,
            billing_period_alignment as billing_period_alignment,
            charged_through_date as charged_through_date,
            charge_model as charge_model,
            charge_number as rate_plan_charge_number,
            charge_type as charge_type,
            description as description,
            discount_level as discount_level,
            dmrc
            as delta_mrc,  -- delta monthly recurring charge
            dtcv
            as delta_tcv,  -- delta total contract value

            is_last_segment as is_last_segment,
            list_price_base as list_price_base,
            overage_calculation_option as overage_calculation_option,
            overage_unused_units_credit_option as overage_unused_units_credit_option,
            processed_through_date as processed_through_date,

            segment as segment,
            specific_billing_period as specific_billing_period,
            specific_end_date as specific_end_date,
            trigger_date as trigger_date,
            trigger_event as trigger_event,
            up_to_periods as up_to_period,
            up_to_periods_type as up_to_periods_type,
            version as version,

            -- ext1, ext2, ext3, ... ext13
            -- metadata
            created_by_id as created_by_id,
            created_date as created_date,
            updated_by_id as updated_by_id,
            updated_date as updated_date,
            _fivetran_deleted as is_deleted

        from source

    )

select *
from renamed
