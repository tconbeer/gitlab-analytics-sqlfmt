with
    source as (select * from {{ source("greenhouse", "job_custom_fields") }}),
    renamed as (

        select

            -- key
            job_id::number as job_id,
            user_id::number as user_id,

            -- info
            custom_field::varchar as job_custom_field,
            float_value::float as job_custom_field_float_value,
            date_value::date as job_custom_field_date_value,
            display_value::varchar as job_custom_field_display_value,
            unit::varchar as job_custom_field_unit,
            min_value::number as job_custom_field_min_value,
            max_value::number as job_custom_field_max_value,
            created_at::timestamp as job_custom_field_created_at,
            updated_at::timestamp as job_custom_field_updated_at

        from source

    )

select *
from renamed
