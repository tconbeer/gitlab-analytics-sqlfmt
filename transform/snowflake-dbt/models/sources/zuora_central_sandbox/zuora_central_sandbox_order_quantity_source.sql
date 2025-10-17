with
    source as (select * from {{ source("zuora_central_sandbox", "order_quantity") }}),
    renamed as (

        select
            id as order_quantity_id,

            invoice_owner_id as invoice_owner_id,
            subscription_owner_id as subscription_owner_id,
            sold_to_contact_id as sold_to_contact_id,
            account_id as account_id,
            order_action_id as order_action_id,
            product_id as product_id,
            subscription_version_amendment_id as subscription_version_amendment_id,
            subscription_id as subscription_id,
            default_payment_method_id as default_payment_method_id,
            rate_plan_charge_id as rate_plan_charge_id,
            product_rate_plan_id as product_rate_plan_id,
            bill_to_contact_id as bill_to_contact_id,
            order_id as order_id,
            rate_plan_id as rate_plan_id,
            product_rate_plan_charge_id as product_rate_plan_charge_id,
            amendment_id as amendment_id,

            start_date as start_date,
            end_date as end_date,
            term as term,
            value as value,

            -- metadata
            updated_by_id as updated_by_id,
            updated_date as updated_date,
            created_by_id as created_by_id,
            created_date as created_date,

            _fivetran_deleted as is_deleted

        from source

    )

select *
from renamed
