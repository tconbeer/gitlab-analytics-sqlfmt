with
    source as (select * from {{ source("xactly", "xc_attainment_measure_criteria") }}),
    renamed as (

        select

            attainment_measure_criteria_id::varchar as attainment_measure_criteria_id,
            attainment_measure_id::varchar as attainment_measure_id,
            created_by_id::float as created_by_id,
            created_date::varchar as created_date,
            criteria_id::float as criteria_id,
            history_uuid::varchar as history_uuid,
            is_active::varchar as is_active,
            modified_by_id::float as modified_by_id,
            modified_date::varchar as modified_date,
            name::varchar as name,
            type::varchar as type

        from source

    )

select *
from renamed
