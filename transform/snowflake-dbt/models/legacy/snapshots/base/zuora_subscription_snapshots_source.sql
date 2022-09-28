-- depends_on: {{ ref('zuora_excluded_accounts') }}
with
    source as (select * from {{ source("snapshots", "zuora_subscription_snapshots") }}),
    renamed as (

        select
            id as subscription_id,
            subscriptionversionamendmentid as amendment_id,
            name as subscription_name,
            {{ zuora_slugify("name") }} as subscription_name_slugify,
            -- keys
            accountid as account_id,
            creatoraccountid as creator_account_id,
            creatorinvoiceownerid as creator_invoice_owner_id,
            invoiceownerid as invoice_owner_id,
            nullif(opportunityid__c, '') as sfdc_opportunity_id,
            nullif(originalid, '') as original_id,
            nullif(previoussubscriptionid, '') as previous_subscription_id,
            nullif(recurlyid__c, '') as sfdc_recurly_id,
            cpqbundlejsonid__qt as cpq_bundle_json_id,

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

            -- Supersonics fields
            iff(
                length(trim(turnoncloudlicensing__c)) > 0, turnoncloudlicensing__c, null
            ) as turn_on_cloud_licensing,
            -- turnonusagepingrequiredmetrics__c           AS
            -- turn_on_usage_ping_required_metrics,
            iff(
                length(trim(turnonoperationalmetrics__c)) > 0,
                turnonoperationalmetrics__c,
                null
            ) as turn_on_operational_metrics,
            iff(
                length(trim(contractoperationalmetrics__c)) > 0,
                contractoperationalmetrics__c,
                null
            ) as contract_operational_metrics,
            iff(
                length(trim(contractautorenew__c)) > 0, contractautorenew__c, null
            ) as contract_auto_renewal,
            iff(
                length(trim(turnonautorenew__c)) > 0, turnonautorenew__c, null
            ) as turn_on_auto_renewal,
            iff(
                length(trim(contractseatreconciliation__c)) > 0,
                contractseatreconciliation__c,
                null
            ) as contract_seat_reconciliation,
            iff(
                length(trim(turnonseatreconciliation__c)) > 0,
                turnonseatreconciliation__c,
                null
            ) as turn_on_seat_reconciliation,

            -- key_dates
            cancelleddate as cancelled_date,
            contractacceptancedate as contract_acceptance_date,
            contracteffectivedate as contract_effective_date,
            initialterm as initial_term,
            initialtermperiodtype as initial_term_period_type,
            termenddate as term_end_date,
            termstartdate as term_start_date,
            subscriptionenddate as subscription_end_date,
            subscriptionstartdate as subscription_start_date,
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

            -- metadata
            updatedbyid as updated_by_id,
            updateddate as updated_date,
            createdbyid as created_by_id,
            createddate as created_date,
            deleted as is_deleted,
            excludefromanalysis__c as exclude_from_analysis,

            -- snapshot metadata
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to

        from source

    )

select *
from renamed
