with
    source as (select * from {{ source("zuora_api_sandbox", "invoice_item") }}),
    renamed as (

        select
            -- keys
            id as invoice_item_id,
            invoiceid as invoice_id,
            appliedtoinvoiceitemid as applied_to_invoice_item_id,
            rateplanchargeid as rate_plan_charge_id,
            subscriptionid as subscription_id,


            -- invoice item metadata
            accountingcode as accounting_code,
            productid as product_id,
            productrateplanchargeid as product_rate_plan_charge_id,

            -- revrecstartdate        AS revenue_recognition_start_date,
            serviceenddate as service_end_date,
            servicestartdate as service_start_date,


            -- financial info
            chargeamount as charge_amount,
            chargedate as charge_date,
            chargename as charge_name,
            processingtype as processing_type,
            quantity as quantity,
            sku as sku,
            taxamount as tax_amount,
            taxcode as tax_code,
            taxexemptamount as tax_exempt_amount,
            taxmode as tax_mode,
            uom as unit_of_measure,
            unitprice as unit_price,

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
