with
    source as (select * from {{ source("zuora_central_sandbox", "invoice_item") }}),
    renamed as (

        select
            -- keys
            id as invoice_item_id,
            invoice_id as invoice_id,
            applied_to_invoice_item_id as applied_to_invoice_item_id,
            rate_plan_charge_id as rate_plan_charge_id,
            subscription_id as subscription_id,


            -- invoice item metadata
            accounting_code as accounting_code,
            product_id as product_id,
            product_rate_plan_charge_id as product_rate_plan_charge_id,
            service_end_date as service_end_date,
            service_start_date as service_start_date,


            -- financial info
            charge_amount as charge_amount,
            charge_date as charge_date,
            charge_name as charge_name,
            processing_type as processing_type,
            quantity as quantity,
            sku as sku,
            tax_amount as tax_amount,
            tax_code as tax_code,
            tax_exempt_amount as tax_exempt_amount,
            tax_mode as tax_mode,
            uom as unit_of_measure,
            unit_price as unit_price,

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
