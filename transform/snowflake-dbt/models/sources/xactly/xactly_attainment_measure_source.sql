with
    source as (select * from {{ source("xactly", "xc_attainment_measure") }}),
    renamed as (

        select

            attainment_measure_id::varchar as attainment_measure_id,
            created_by_id::float as created_by_id,
            created_date::varchar as created_date,
            description::varchar as description,
            effective_end_period_id::float as effective_end_period_id,
            effective_start_period_id::float as effective_start_period_id,
            history_uuid::varchar as history_uuid,
            is_active::varchar as is_active,
            master_attainment_measure_id::varchar as master_attainment_measure_id,
            modified_by_id::float as modified_by_id,
            modified_date::varchar as modified_date,
            name::varchar as name,
            period_type::varchar as period_type

        from source

    )

select *
from renamed
