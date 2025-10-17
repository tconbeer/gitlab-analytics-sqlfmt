with
    source as (select * from {{ source("greenhouse", "offer_custom_fields") }}),
    renamed as (

        select
            -- keys
            offer_id::number as offer_id,
            user_id::number as user_id,

            -- info
            custom_field::varchar as offer_custom_field,
            float_value::float as offer_custom_field_float_value,
            date_value::date as offer_custom_field_date,
            display_value::varchar as offer_custom_field_display_value,
            unit::varchar as offer_custom_field_unit,
            min_value::number as offer_custom_field_min_value,
            max_value::number as offer_custom_field_max_value,
            created_at::timestamp as offer_custom_field_created_at,
            updated_at::timestamp as offer_custom_field_updated_at

        from source

    )

select *
from renamed
