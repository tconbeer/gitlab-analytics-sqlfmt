with
    source as (select * from {{ source("xactly", "xc_pos_relations_hist") }}),
    renamed as (

        select

            created_by_id::float as created_by_id,
            created_by_name::varchar as created_by_name,
            created_date::varchar as created_date,
            from_pos_id::float as from_pos_id,
            from_pos_name::varchar as from_pos_name,
            id::float as id,
            modified_by_id::float as modified_by_id,
            modified_by_name::varchar as modified_by_name,
            modified_date::varchar as modified_date,
            object_id::float as object_id,
            pos_rel_type_id::float as pos_rel_type_id,
            to_pos_id::float as to_pos_id,
            to_pos_name::varchar as to_pos_name

        from source

    )

select *
from renamed
