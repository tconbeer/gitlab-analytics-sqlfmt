with
    source as (select * from {{ source("greenhouse", "application_custom_fields") }}),
    renamed as (

        select

            -- keys
            application_id::number as application_id,
            user_id::number as user_id,

            -- info
            custom_field::varchar as application_custom_field,
            float_value::float as application_custom_field_float_value,
            display_value::varchar as application_custom_field_display_value,
            unit::varchar as application_custom_field_unit,
            min_value::number as application_custom_field_min_value,
            max_value::number as application_custom_field_max_value,
            try_to_date(date_value::varchar) as application_custom_field_date,
            created_at::timestamp as application_custom_field_created_at,
            updated_at::timestamp as application_custom_field_updated_at

        from source

    )

select *
from renamed
