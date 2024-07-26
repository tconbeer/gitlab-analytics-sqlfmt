with
    source as (select * from {{ source("zuora_api_sandbox", "order") }}),
    renamed as (

        select

            id as dim_order_id,

            -- keys
            parentaccountid as parent_account_id,
            billtocontactid as bill_to_contact_id,
            soldtocontactid as sold_to_contact_id,
            defaultpaymentmethodid as default_payment_method_id,

            -- account_info
            orderdate::date as order_date,
            ordernumber as order_number,
            description as order_description,
            state as order_state,
            status as order_status,
            createdbymigration as is_created_by_migration,

            -- metadata
            createdbyid as order_created_by_id,
            createddate::date as order_created_date,
            deleted as is_deleted,
            updatedbyid as update_by_id,
            updateddate::date as updated_date

        from source

    )

select *
from renamed
