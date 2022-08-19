with
    source as (select * from {{ source("xactly", "xc_plan_assignment") }}),

    renamed as (

        select

            plan_assignment_id::float as plan_assignment_id,
            version::float as version,
            assignment_id::float as assignment_id,
            assignment_type::float as assignment_type,
            assignment_name::varchar as assignment_name,
            is_active::varchar as is_active,
            created_date::varchar as created_date,
            created_by_id::float as created_by_id,
            created_by_name::varchar as created_by_name,
            modified_date::varchar as modified_date,
            modified_by_id::float as modified_by_id,
            modified_by_name::varchar as modified_by_name,
            plan_id::float as plan_id,
            active_start_date::varchar as active_start_date,
            active_end_date::varchar as active_end_date

        from source

    )

select *
from renamed
