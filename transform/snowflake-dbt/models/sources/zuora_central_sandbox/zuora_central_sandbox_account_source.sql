with
    source as (select * from {{ source("zuora_central_sandbox", "account") }}),
    renamed as (

        select
            id as account_id,
            -- keys
            communication_profile_id as communication_profile_id,
            nullif(
                "{{this.database}}".{{ target.schema }}.id15to18(ifnull(crm_id, '')), ''
            ) as crm_id,

            default_payment_method_id as default_payment_method_id,
            invoice_template_id as invoice_template_id,
            parent_id as parent_id,
            sold_to_contact_id as sold_to_contact_id,
            bill_to_contact_id as bill_to_contact_id,
            tax_exempt_certificate_id as tax_exempt_certificate_id,
            tax_exempt_certificate_type as tax_exempt_certificate_type,

            -- account info
            account_number as account_number,
            name as account_name,
            notes as account_notes,
            purchase_order_number as purchase_order_number,
            account_code_c as sfdc_account_code,
            status as status,
            entity_c as sfdc_entity,

            auto_pay as auto_pay,
            balance as balance,
            credit_balance as credit_balance,
            bill_cycle_day as bill_cycle_day,
            currency as currency,
            conversion_rate_c as sfdc_conversion_rate,
            payment_term as payment_term,

            allow_invoice_edit as allow_invoice_edit,
            batch as batch,
            invoice_delivery_prefs_email as invoice_delivery_prefs_email,
            invoice_delivery_prefs_print as invoice_delivery_prefs_print,
            payment_gateway as payment_gateway,

            customer_service_rep_name as customer_service_rep_name,
            sales_rep_name as sales_rep_name,
            additional_email_addresses as additional_email_addresses,
            parent_c as sfdc_parent,
            sspchannel_c as ssp_channel,
            porequired_c as po_required,

            -- financial info
            last_invoice_date as last_invoice_date,

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
