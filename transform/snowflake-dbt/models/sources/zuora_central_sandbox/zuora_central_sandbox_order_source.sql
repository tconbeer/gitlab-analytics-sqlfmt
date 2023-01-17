with
    source as (select * from {{ source("zuora_central_sandbox", "order") }}),
    renamed as (

        select

            id as order_id,

            description as description,
            order_date as order_date,
            order_number as order_number,
            state as state,
            status as status,

            sold_to_contact_id as sold_to_contact_id,

            account_id as account_id,
            bill_to_contact_id as bill_to_contact_id,
            default_payment_method_id as default_payment_method_id,

            -- metadata
            updated_by_id as updated_by_id,
            updated_date as updated_date,
            created_by_id as created_by_id,
            created_date as created_date,
            created_by_migration as created_by_migration,

            _fivetran_deleted as is_deleted

        from source

    )

select *
from renamed
