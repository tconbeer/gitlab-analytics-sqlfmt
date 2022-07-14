with
    source as (

        select *
        from {{ source("license", "granted_add_ons") }}
        qualify
            row_number() OVER (partition by id order by updated_at::timestamp desc) = 1

    ),
    renamed as (

        select
            id::number as granted_add_on_id,
            license_id::number as license_id,
            add_on_id::number as add_on_id,
            quantity::number as quantity,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at
        from source

    )

select *
from renamed
