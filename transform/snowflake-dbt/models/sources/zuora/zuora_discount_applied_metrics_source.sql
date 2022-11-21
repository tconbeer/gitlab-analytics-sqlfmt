-- values to consider renaming:
-- mrr
-- dmrc
-- dtcv
-- tcv
-- uom
with
    source as (select * from {{ source("zuora", "discount_applied_metrics") }}),
    renamed as (

        select
            id as discount_applied_metrics_id,

            -- keys 
            accountid as account_id,
            amendmentid as amendment_id,
            billtocontactid as bill_to_contact_id,
            defaultpaymentmethodid as default_payment_method_id,
            discountrateplanchargeid as discount_rate_plan_charge_id,
            parentaccountid as parent_account_id,
            productid as product_id,
            productrateplanchargeid as product_rate_plan_charge_id,
            productrateplanid as product_rate_plan_id,
            rateplanchargeid as rate_plan_charge_id,
            rateplanid as rate_plan_id,
            soldtocontactid as sold_to_contact_id,
            subscriptionid as subscription_id,

            -- info 
            date_trunc('month', enddate)::date as end_date,
            date_trunc('month', startdate)::date as start_date,
            mrr,
            tcv,

            -- metadata 
            createdbyid as created_by_id,
            createddate as created_date,

            updatedbyid as updated_by_id,
            updateddate as updated_date
        from source

    )

select *
from renamed
