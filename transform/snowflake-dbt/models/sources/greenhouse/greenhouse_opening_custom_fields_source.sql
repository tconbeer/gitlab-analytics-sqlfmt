with
    source as (select * from {{ source("greenhouse", "opening_custom_fields") }}),
    renamed as (

        select

            -- key
            opening_id::varchar as opening_id,

            -- info
            key::varchar as opening_custom_field,
            display_value::varchar as opening_custom_field_display_value,
            created_at::timestamp as opening_custom_field_created_at,
            updated_at::timestamp as opening_custom_field_updated_at
        from source

    )

select *
from renamed
