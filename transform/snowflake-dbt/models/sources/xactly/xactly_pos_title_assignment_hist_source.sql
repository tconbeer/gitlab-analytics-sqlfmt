with
    source as (select * from {{ source("xactly", "xc_pos_title_assignment_hist") }}),
    renamed as (

        select

            created_by_id::float as created_by_id,
            created_by_name::varchar as created_by_name,
            created_date::varchar as created_date,
            is_active::varchar as is_active,
            modified_by_id::float as modified_by_id,
            modified_by_name::varchar as modified_by_name,
            modified_date::varchar as modified_date,
            object_id::float as object_id,
            pos_title_assignment_id::float as pos_title_assignment_id,
            position_id::float as position_id,
            position_name::varchar as position_name,
            title_id::float as title_id,
            title_name::varchar as title_name

        from source

    )

select *
from renamed
