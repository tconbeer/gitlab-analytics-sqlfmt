with
    source as (select * from {{ source("greenhouse", "attributes") }}),
    renamed as (

        select

            -- keys
            id::number as attribute_id,
            organization_id::number as organization_id,

            -- info
            name::varchar as attribute_name,
            category::varchar as attribute_category,
            created_at::varchar::timestamp as attribute_created_at,
            updated_at::varchar::timestamp as attribute_updated_at

        from source

    )

select *
from renamed
