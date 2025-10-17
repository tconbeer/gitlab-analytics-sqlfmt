with
    source as (select * from {{ source("netsuite", "budget") }}),
    renamed as (

        select
            -- Primary Key
            budget_id::float as budget_id,

            -- Foreign Keys
            accounting_period_id::float as accounting_period_id,
            account_id::float as account_id,
            department_id::float as department_id,
            subsidiary_id::float as subsidiary_id,
            category_id::float as category_id,

            -- Info
            amount::float as budget_amount,
            _fivetran_deleted::boolean as is_fivetran_deleted

        from source

    )

select *
from renamed
