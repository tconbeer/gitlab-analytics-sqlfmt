with
    source as (select * from {{ source("greenhouse", "sources") }}),
    renamed as (

        select
            id as source_id,

            -- keys
            organization_id,
            name::varchar(250) as source_name,
            type::varchar(250) as source_type,

            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at

        from source
    )

select *
from renamed
