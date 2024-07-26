{{ config(tags=["mnpi"]) }}

with
    source as (select * from {{ source("salesforce", "zqu_quote_rate_plan_charge") }}),
    renamed as (

        select

            -- ids
            id as zqu_quote_rate_plan_charge_id,
            name as zqu_quote_rate_plan_charge_name,
            zqu__description__c as zqu_quote_rate_plan_charge_description,

            -- info
            discount_on_quote_formula__c as discount_on_quote_formula,
            discount_text__c as discount_text,
            effective_annual_price__c as effective_annual_price,
            list_annual_price__c as list_annual_price,
            quote__c as zqu_quote_id,
            discount_on_quote_safe_for_quote__c as discount_on_quote_safe_for_quote,
            list_price_safe_for_quote__c as list_price_safe_for_quote,
            one__c as one,
            mavenlink_project_template_id__c as mavenlink_project_template_id,
            undiscounted_tcv__c as undiscounted_tcv,
            zqu__apply_discount_to_one_time_charges__c
            as zqu_apply_discount_to_one_time_charges,
            zqu__apply_discount_to_recurring_charges__c
            as zqu__apply_discount_to_recurring_charges,
            zqu__apply_discount_to_usage_charges__c as apply_discount_to_usage_charges,
            zqu__billcycleday__c as zqu_bill_cycle_day,
            zqu__billcycletype__c as zqu_bill_cycle_type,
            zqu__billingdiscount__c as zqu_billing_discount,
            zqu__billingsubtotal__c as zqu_billing_sub_total,
            zqu__billingtax__c as zqu_billing_tax,
            zqu__billingtotal__c as zqu_billing_total,
            zqu__chargetype__c as zqu_charge_type,
            zqu__currency__c as zqu_currency,
            zqu__discount_level__c as zqu_discount_level,
            zqu__discount__c as zqu_discount,
            zqu__effectiveprice__c as zqu_effective_price,
            zqu__enddatecondition__c as zqu_end_date_condition,
            zqu__feetype__c as zqu_fee_type,
            zqu__islastsegment__c as zqu_is_last_segment,
            zqu__listpricebase__c as zqu_list_price_base,
            zqu__listprice__c as zqu_list_price,
            zqu__listtotal__c as zqu_list_total,
            zqu__model__c as zqu_model,
            zqu__mrr__c as zqu_mrr,
            zqu__period__c as zqu_period,
            zqu__previewedmrr__c as zqu_previewed_mrr,
            zqu__previewedtcv__c as zqu_previewed_tcv,
            zqu__productname__c as zqu_product_name,
            zqu__productrateplancharge__c as zqu_product_rate_plan_charge_id,
            zqu__productrateplanchargezuoraid__c
            as zqu_product_rate_plan_charge_zuora_id,
            zqu__quantity__c as zqu_quantity,
            zqu__quoterateplan__c as zqu_quote_rate_plan_id,
            zqu__rateplanname__c as zqu_rate_plan_name,
            zqu__specificbillingperiod__c as zqu_specific_billing_period,
            zqu__subscriptionrateplanchargezuoraid__c
            as zqu_subscription_rate_plan_charge_zuora_id,
            zqu__tcv__c as zqu_tcv,
            zqu__total__c as zqu_total,
            zqu__uom__c as zqu_uom,
            zqu__upto_how_many_periods_type__c as zqu_up_to_how_many_periods_type,
            zqu__upto_how_many_periods__c as zqu_up_to_how_many_periods,

            -- metadata
            createdbyid as created_by_id,
            createddate as created_date,
            isdeleted as is_deleted,
            lastmodifiedbyid as last_modified_by_id,
            lastmodifieddate as last_modified_date,
            _sdc_received_at as sfdc_received_at,
            _sdc_extracted_at as sfdc_extracted_at,
            _sdc_table_version as sfdc_table_version,
            _sdc_batched_at as sfdc_batched_at,
            _sdc_sequence as sfdc_sequence,
            systemmodstamp as system_mod_stamp

        from source
    )

select *
from renamed
