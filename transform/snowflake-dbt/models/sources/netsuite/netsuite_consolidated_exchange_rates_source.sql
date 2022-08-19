with
    source as (select * from {{ source("netsuite", "consolidated_exchange_rates") }}),
    renamed as (

        select
            -- Primary Key
            consolidated_exchange_rate_id::float as consolidated_exchange_rate_id,

            -- Foreign Keys
            accounting_period_id::float as accounting_period_id,
            from_subsidiary_id::float as from_subsidiary_id,
            to_subsidiary_id::float as to_subsidiary_id,

            -- Info
            accounting_book_id::float as accounting_book_id,
            average_budget_rate::float as average_budget_rate,
            current_budget_rate::float as current_budget_rate,
            average_rate::float as average_rate,
            current_rate::float as current_rate,
            historical_budget_rate::float as historical_budget_rate,
            historical_rate::float as historical_rate,
            _fivetran_deleted::boolean as is_fivetran_deleted


        from source


    )

select *
from renamed
