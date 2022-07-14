with
    source as (

        select *
        from {{ source("customers", "customers_db_eulas") }}
        qualify row_number() OVER (partition by id order by updated_at desc) = 1

    ),
    renamed as (

        select
            id::number as eula_id,
            name::varchar as eula_name,
            content::varchar as eula_content,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at
        from source

    )

select *
from renamed
