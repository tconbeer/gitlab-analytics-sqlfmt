with
    source as (select * from {{ source("xactly", "xc_credit_adjustment") }}),
    renamed as (

        select

            amount::float as amount,
            amount_unit_type_id::float as amount_unit_type_id,
            created_by_id::float as created_by_id,
            created_by_name::varchar as created_by_name,
            created_date::varchar as created_date,
            credit_adjustment_id::float as credit_adjustment_id,
            credit_id::float as credit_id,
            is_active::varchar as is_active,
            modified_by_id::float as modified_by_id,
            modified_by_name::varchar as modified_by_name,
            modified_date::varchar as modified_date,
            period_id::float as period_id,
            reason_id::float as reason_id

        from source

    )

select *
from renamed
