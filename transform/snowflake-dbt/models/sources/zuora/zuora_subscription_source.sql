{{
    config(
        {
            "alias": "zuora_subscription_source",
            "post-hook": '{{ apply_dynamic_data_masking(columns = [{"account_id":"string"},{"created_by_id":"string"},{"creator_account_id":"string"},{"creator_invoice_owner_id":"string"},{"namespace_id":"string"},{"namespace_name":"string"},{"subscription_id":"string"},{"invoice_owner_id":"string"},{"subscription_name":"string"},{"notes":"string"},{"subscription_name":"string"},{"opportunity_name":"string"},{"subscription_name":"string"},{"original_id":"string"},{"previous_subscription_id":"string"},{"sfdc_purchase_order":"string"},{"quote_number":"string"},{"quote_type":"string"},{"sfdc_recurly_id":"string"},{"amendment_id":"string"},{"updated_by_id":"string"},{"crm_opportunity_name":"string"},{"subscription_name_slugify":"string"},{"sfdc_opportunity_id":"string"}]) }}',
        }
    )
}}

-- depends_on: {{ ref('zuora_excluded_accounts') }}
with
    source as (select * from {{ source("zuora", "subscription") }}),
    renamed as (

        select
            id as subscription_id,
            subscriptionversionamendmentid as amendment_id,
            name as subscription_name,
            {{ zuora_slugify("name") }} as subscription_name_slugify,
            nullif(gitlabnamespacename__c, '') as namespace_name,
            -- keys
            accountid as account_id,
            creatoraccountid as creator_account_id,
            creatorinvoiceownerid as creator_invoice_owner_id,
            invoiceownerid as invoice_owner_id,
            nullif(opportunityid__c, '') as sfdc_opportunity_id,
            nullif(opportunityname__qt, '') as crm_opportunity_name,
            nullif(originalid, '') as original_id,
            nullif(previoussubscriptionid, '') as previous_subscription_id,
            nullif(recurlyid__c, '') as sfdc_recurly_id,
            cpqbundlejsonid__qt as cpq_bundle_json_id,
            nullif(gitlabnamespaceid__c, '') as namespace_id,

            -- info
            status as subscription_status,
            autorenew as auto_renew_native_hist,
            autorenew__c as auto_renew_customerdot_hist,
            version as version,
            termtype as term_type,
            notes as notes,
            isinvoiceseparate as is_invoice_separate,
            currentterm as current_term,
            currenttermperiodtype as current_term_period_type,
            endcustomerdetails__c as sfdc_end_customer_details,
            eoastarterbronzeofferaccepted__c as eoa_starter_bronze_offer_accepted,
            turnoncloudlicensing__c as turn_on_cloud_licensing,
            -- turnonusagepingrequiredmetrics__c           AS
            -- turn_on_usage_ping_required_metrics,
            turnonoperationalmetrics__c as turn_on_operational_metrics,
            contractoperationalmetrics__c as contract_operational_metrics,

            -- key_dates
            cancelleddate as cancelled_date,
            contractacceptancedate as contract_acceptance_date,
            contracteffectivedate as contract_effective_date,
            initialterm as initial_term,
            initialtermperiodtype as initial_term_period_type,
            termenddate::date as term_end_date,
            termstartdate::date as term_start_date,
            subscriptionenddate::date as subscription_end_date,
            subscriptionstartdate::date as subscription_start_date,
            serviceactivationdate as service_activiation_date,
            opportunityclosedate__qt as opportunity_close_date,
            originalcreateddate as original_created_date,

            -- foreign synced info
            opportunityname__qt as opportunity_name,
            purchase_order__c as sfdc_purchase_order,
            -- purchaseorder__c                            AS sfdc_purchase_order_,
            quotebusinesstype__qt as quote_business_type,
            quotenumber__qt as quote_number,
            quotetype__qt as quote_type,

            -- renewal info
            renewalsetting as renewal_setting,
            renewal_subscription__c__c as zuora_renewal_subscription_name,

            split(
                nullif({{ zuora_slugify("renewal_subscription__c__c") }}, ''), '|'
            ) as zuora_renewal_subscription_name_slugify,
            renewalterm as renewal_term,
            renewaltermperiodtype as renewal_term_period_type,
            exclude_from_renewal_report__c__c as exclude_from_renewal_report,
            contractautorenew__c as contract_auto_renewal,
            turnonautorenew__c as turn_on_auto_renewal,
            contractseatreconciliation__c as contract_seat_reconciliation,
            turnonseatreconciliation__c as turn_on_seat_reconciliation,

            -- metadata
            updatedbyid as updated_by_id,
            updateddate as updated_date,
            createdbyid as created_by_id,
            createddate as created_date,
            deleted as is_deleted,
            excludefromanalysis__c as exclude_from_analysis

        from source

    )

select *
from renamed
