with
    source as (

        select * from {{ source("zuora_api_sandbox", "revenue_schedule_item") }}

    ),
    renamed as (

        select
            -- Primary Keys
            id::varchar as revenue_schedule_item_id,

            -- Foreign Keys
            accountid::varchar as account_id,
            parentaccountid::varchar as parent_account_id,
            accountingperiodid::varchar as accounting_period_id,
            amendmentid::varchar as amendment_id,
            subscriptionid::varchar as subscription_id,
            productid::varchar as product_id,
            rateplanchargeid::varchar as rate_plan_charge_id,
            rateplanid::varchar as rate_plan_id,
            soldtocontactid::varchar as sold_to_contact_id,

            -- Info
            amount::float as revenue_schedule_item_amount,
            billtocontactid::varchar as bill_to_contact_id,
            currency::varchar as currency,
            createdbyid::varchar as created_by_id,
            createddate::timestamp_tz as created_date,
            defaultpaymentmethodid::varchar as default_payment_method_id,
            deleted::boolean as is_deleted,
            updatedbyid::varchar as updated_by_id,
            updateddate::timestamp_tz as updated_date

        from source

    )

select *
from renamed
