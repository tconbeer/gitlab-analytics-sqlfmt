-- depends_on: {{ ref('zuora_excluded_accounts') }}
with
    source as (select * from {{ source("snapshots", "zuora_account_snapshots") }}),
    zuora_account as (select * from {{ source("zuora", "account") }}),
    renamed as (

        select
            id as account_id,
            -- keys
            communicationprofileid as communication_profile_id,
            nullif(
                "{{this.database}}".{{ target.schema }}.id15to18(crmid), ''
            ) as crm_id,
            defaultpaymentmethodid as default_payment_method_id,
            invoicetemplateid as invoice_template_id,
            parentid as parent_id,
            soldtocontactid as sold_to_contact_id,
            billtocontactid as bill_to_contact_id,
            taxexemptcertificateid as tax_exempt_certificate_id,
            taxexemptcertificatetype as tax_exempt_certificate_type,

            -- account info
            accountnumber as account_number,
            name as account_name,
            notes as account_notes,
            purchaseordernumber as purchase_order_number,
            accountcode__c as sfdc_account_code,
            status,
            entity__c as sfdc_entity,

            autopay as auto_pay,
            balance as balance,
            creditbalance as credit_balance,
            billcycleday as bill_cycle_day,
            currency as currency,
            conversionrate__c as sfdc_conversion_rate,
            paymentterm as payment_term,

            allowinvoiceedit as allow_invoice_edit,
            batch,
            invoicedeliveryprefsemail as invoice_delivery_prefs_email,
            invoicedeliveryprefsprint as invoice_delivery_prefs_print,
            paymentgateway as payment_gateway,

            customerservicerepname as customer_service_rep_name,
            salesrepname as sales_rep_name,
            additionalemailaddresses as additional_email_addresses,
            -- billtocontact                   as bill_to_contact,
            parent__c as sfdc_parent,

            sspchannel__c as ssp_channel,
            porequired__c as po_required,

            -- financial info
            lastinvoicedate as last_invoice_date,

            -- metadata
            createdbyid as created_by_id,
            createddate as created_date,
            updatedbyid as updated_by_id,
            updateddate as updated_date,
            deleted as is_deleted,

            -- snapshot metadata
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to

        from source

    ),
    final as (

        select renamed.*, zuora_account.batch as live_batch
        from renamed
        left join zuora_account on renamed.account_id = zuora_account.id
    )

select *
from final
