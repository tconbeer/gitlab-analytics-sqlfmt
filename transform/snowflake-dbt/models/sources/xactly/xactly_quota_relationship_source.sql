with
    source as (select * from {{ source("xactly", "xc_quota_relationship") }}),
    renamed as (

        select

            created_by_id::float as created_by_id,
            created_date::varchar as created_date,
            description::varchar as description,
            is_active::varchar as is_active,
            label::varchar as label,
            modified_by_id::float as modified_by_id,
            modified_date::varchar as modified_date,
            name::varchar as name,
            quota_relationship_id::varchar as quota_relationship_id

        from source

    )

select *
from renamed
