with
    source as (

        select *
        from {{ source("customers", "customers_db_eula_requests") }}
        qualify row_number() OVER (partition by id order by updated_at desc) = 1

    ),
    renamed as (

        select
            id::number as eula_request_id,
            eula_id::number as eula_id,
            customer_id::number as customer_id,
            subscription_id::varchar as zuora_subscription_id,
            subscription_name::varchar as zuora_subscription_name,
            eula_type::number as eula_type,
            accepted_at::timestamp as accepted_at,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at
        from source

    )

select *
from renamed
