with
    source as (select * from {{ source("xactly", "xc_credit_held") }}),
    renamed as (

        select

            created_by_id::float as created_by_id,
            created_by_name::varchar as created_by_name,
            created_date::varchar as created_date,
            credit_held_id::float as credit_held_id,
            credit_id::float as credit_id,
            held_date::varchar as held_date,
            is_active::varchar as is_active,
            is_held::varchar as is_held,
            modified_by_id::float as modified_by_id,
            modified_by_name::varchar as modified_by_name,
            modified_date::varchar as modified_date,
            release_group_id::float as release_group_id,
            run_id::float as run_id,
            trans_id::float as trans_id

        from source

    )

select *
from renamed
