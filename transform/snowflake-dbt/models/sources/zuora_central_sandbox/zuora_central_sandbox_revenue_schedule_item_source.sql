with
    source as (

        select * from {{ source("zuora_central_sandbox", "revenue_schedule_item") }}

    ),
    renamed as (

        select
            -- Primary Keys
            id::varchar as revenue_schedule_item_id,

            -- Foreign Keys
            account_id::varchar as account_id,
            -- parent_account_id::VARCHAR                    AS parent_account_id,
            accounting_period_id::varchar as accounting_period_id,
            amendment_id::varchar as amendment_id,
            subscription_id::varchar as subscription_id,
            product_id::varchar as product_id,
            rate_plan_charge_id::varchar as rate_plan_charge_id,
            rate_plan_id::varchar as rate_plan_id,
            sold_to_contact_id::varchar as sold_to_contact_id,

            -- Info
            amount::float as revenue_schedule_item_amount,
            bill_to_contact_id::varchar as bill_to_contact_id,
            currency::varchar as currency,
            created_by_id::varchar as created_by_id,
            created_date::timestamp_tz as created_date,
            default_payment_method_id::varchar as default_payment_method_id,
            _fivetran_deleted::boolean as is_deleted,
            updated_by_id::varchar as updated_by_id,
            updated_date::timestamp_tz as updated_date

        from source

    )

select *
from renamed
