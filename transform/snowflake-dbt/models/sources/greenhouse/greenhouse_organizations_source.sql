with
    source as (select * from {{ source("greenhouse", "organizations") }}),
    renamed as (

        -- key
        -- info
        select id::number as organization_id, name::varchar as organization_name

        from source

    )

select *
from renamed
