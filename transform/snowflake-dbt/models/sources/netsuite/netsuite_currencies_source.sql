with
    source as (select * from {{ source("netsuite", "currencies") }}),
    renamed as (

        select
            -- Primary Key
            currency_id::float as currency_id,

            -- Info
            name::varchar as currency_name,
            precision_0::float as decimal_precision,
            symbol::varchar as currency_symbol,

            -- Meta
            is_inactive::boolean as is_currency_inactive,
            _fivetran_deleted::boolean as is_fivetran_deleted


        from source

    )

select *
from renamed
