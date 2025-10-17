with
    source as (

        select *
        from {{ source("version", "versions") }}
        qualify row_number() over (partition by id order by updated_at desc) = 1

    ),
    renamed as (

        select
            id::number as id,
            version::varchar as version,
            vulnerable::boolean as is_vulnerable,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at
        from source

    )

select *
from renamed
