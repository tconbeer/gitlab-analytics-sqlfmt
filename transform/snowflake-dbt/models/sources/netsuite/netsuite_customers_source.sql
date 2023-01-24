with
    source as (select * from {{ source("netsuite", "customers") }}),
    renamed as (

        select
            -- Primary Key
            customer_id::float as customer_id,

            -- Foreign Keys
            subsidiary_id::float as subsidiary_id,
            currency_id::float as currency_id,
            parent_id::float as parent_id,
            department_id::float as department_id,

            -- Info
            companyname::varchar as customer_name,
            name::varchar as customer_alt_name,
            full_name::varchar as customer_full_name,
            rev_rec_forecast_rule_id::float as rev_rec_forecast_rule_id,

            -- deposit_balance_foreign
            openbalance::float as customer_balance,
            days_overdue::float as days_overdue,
            _fivetran_deleted as is_fivetran_deleted

        from source

    )

select *
from renamed
