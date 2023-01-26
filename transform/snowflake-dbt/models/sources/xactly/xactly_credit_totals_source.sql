with
    source as (select * from {{ source("xactly", "xc_credit_totals") }}),
    renamed as (

        select

            amount::float as amount,
            created_by_id::float as created_by_id,
            created_by_name::varchar as created_by_name,
            created_date::varchar as created_date,
            credit_totals_id::float as credit_totals_id,
            is_active::varchar as is_active,
            modified_by_id::float as modified_by_id,
            modified_by_name::varchar as modified_by_name,
            modified_date::varchar as modified_date,
            participant_id::float as participant_id,
            participant_name::varchar as participant_name,
            period_id::float as period_id,
            position_id::float as position_id,
            position_name::varchar as position_name,
            unittype_id::float as unittype_id

        from source

    )

select *
from renamed
