with
    source as (

        select * from {{ source("greenhouse", "rejection_question_custom_fields") }}

    ),
    renamed as (

        select

            -- key
            application_id::number as application_id,
            user_id::number as user_id,

            -- info
            custom_field::varchar as rejection_question_custom_field,
            float_value::float as rejection_question_custom_field_float_value,
            try_to_date(
                date_value::varchar
            ) as rejection_question_custom_field_date_value,
            display_value::varchar as rejection_question_custom_field_display_value,
            unit::varchar as rejection_question_custom_field_unit,
            min_value::number as rejection_question_custom_field_min_value,
            max_value::number as rejection_question_custom_field_max_value,
            created_at::timestamp as rejection_question_custom_field_created_at,
            updated_at::timestamp as rejection_question_custom_field_updated_at

        from source

    )

select *
from renamed
