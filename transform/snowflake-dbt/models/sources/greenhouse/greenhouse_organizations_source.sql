with
    source as (select * from {{ source("greenhouse", "organizations") }}),
    renamed as (

        select
            -- key
            id::number as organization_id,

            -- info
            name::varchar as organization_name

        from source

    )

select *
from renamed
