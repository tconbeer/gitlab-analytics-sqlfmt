with
    source as (

        select *
        from {{ source("license", "add_ons") }}
        qualify
            row_number() over (partition by id order by updated_at::timestamp desc) = 1

    ),
    renamed as (

        select
            id::number as add_on_id,
            name::varchar as add_on_name,
            code::varchar as add_on_code,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at
        from source

    )

select *
from renamed
