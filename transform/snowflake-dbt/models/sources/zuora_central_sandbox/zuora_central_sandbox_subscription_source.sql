with
    source as (select * from {{ source("zuora_central_sandbox", "subscription") }}),
    renamed as (

        select
            id as subscription_id,
            subscription_version_amendment_id as amendment_id,
            name as subscription_name,
            {{ zuora_slugify("name") }} as subscription_name_slugify,
            -- keys
            account_id as account_id,
            creator_account_id as creator_account_id,
            creator_invoice_owner_id as creator_invoice_owner_id,
            invoice_owner_id as invoice_owner_id,
            nullif(opportunity_id_c, '') as sfdc_opportunity_id,
            nullif(opportunity_name_qt, '') as crm_opportunity_name,
            nullif(original_id, '') as original_id,
            nullif(previous_subscription_id, '') as previous_subscription_id,
            nullif(recurly_id_c, '') as sfdc_recurly_id,
            cpq_bundle_json_id_qt as cpq_bundle_json_id,

            -- info
            status as subscription_status,
            auto_renew as auto_renew_native_hist,
            auto_renew_c as auto_renew_customerdot_hist,
            version as version,
            term_type as term_type,
            notes as notes,
            is_invoice_separate as is_invoice_separate,
            current_term as current_term,
            current_term_period_type as current_term_period_type,
            end_customer_details_c as sfdc_end_customer_details,
            eoa_starter_bronze_offer_accepted_c as eoa_starter_bronze_offer_accepted,
            iff(
                length(trim(turn_on_cloud_licensing_c)) > 0,
                turn_on_cloud_licensing_c,
                null
            ) as turn_on_cloud_licensing,
            turn_on_seat_reconciliation_c as turn_on_seat_reconciliation,
            turn_on_auto_renew_c as turn_on_auto_renewal,
            turn_on_operational_metrics_c as turn_on_operational_metrics,
            contract_operational_metrics_c as contract_operational_metrics,

            -- key_dates
            cancelled_date as cancelled_date,
            contract_acceptance_date as contract_acceptance_date,
            contract_effective_date as contract_effective_date,
            initial_term as initial_term,
            initial_term_period_type as initial_term_period_type,
            term_end_date::date as term_end_date,
            term_start_date::date as term_start_date,
            subscription_end_date::date as subscription_end_date,
            subscription_start_date::date as subscription_start_date,
            service_activation_date as service_activiation_date,
            opportunity_close_date_qt as opportunity_close_date,
            original_created_date as original_created_date,

            -- foreign synced info
            opportunity_name_qt as opportunity_name,
            purchase_order_c as sfdc_purchase_order,
            quote_business_type_qt as quote_business_type,
            quote_number_qt as quote_number,
            quote_type_qt as quote_type,

            -- renewal info
            renewal_setting as renewal_setting,
            renewal_subscription_c_c as zuora_renewal_subscription_name,
            split(
                nullif({{ zuora_slugify("renewal_subscription_c_c") }}, ''), '|'
            ) as zuora_renewal_subscription_name_slugify,
            renewal_term as renewal_term,
            renewal_term_period_type as renewal_term_period_type,
            contract_auto_renew_c as contract_auto_renewal,
            contract_seat_reconciliation_c as contract_seat_reconciliation,

            -- metadata
            updated_by_id as updated_by_id,
            updated_date as updated_date,
            created_by_id as created_by_id,
            created_date as created_date,
            _fivetran_deleted as is_deleted,
            excludefrom_analysis_c as exclude_from_analysis

        from source

    )

select *
from renamed
