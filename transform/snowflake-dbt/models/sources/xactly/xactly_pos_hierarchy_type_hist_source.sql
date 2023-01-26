with
    source as (select * from {{ source("xactly", "xc_pos_hierarchy_type_hist") }}),
    renamed as (

        select

            created_by_id::float as created_by_id,
            created_by_name::varchar as created_by_name,
            created_date::varchar as created_date,
            descr::varchar as descr,
            effective_end_date::varchar as effective_end_date,
            effective_start_date::varchar as effective_start_date,
            is_active::varchar as is_active,
            is_master::varchar as is_master,
            modified_by_id::float as modified_by_id,
            modified_by_name::varchar as modified_by_name,
            modified_date::varchar as modified_date,
            object_id::float as object_id,
            pos_hierarchy_type_disp_name::varchar as pos_hierarchy_type_disp_name,
            pos_hierarchy_type_id::float as pos_hierarchy_type_id,
            pos_hierarchy_type_name::varchar as pos_hierarchy_type_name

        from source

    )

select *
from renamed
