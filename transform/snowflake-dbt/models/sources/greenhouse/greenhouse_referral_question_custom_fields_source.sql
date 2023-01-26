with
    source as (

        select * from {{ source("greenhouse", "referral_question_custom_fields") }}

    ),
    renamed as (

        select

            -- keys
            person_id::number as candidate_id,
            user_id::number as user_id,

            -- info
            custom_field::varchar as referral_question_custom_field,
            float_value::float as referral_question_custom_field_float_value,
            date_value::varchar::date as referral_question_custom_field_date_value,
            display_value::varchar as referral_question_custom_field_display_value,
            unit::varchar as referral_question_custom_field_unit,
            min_value::number as referral_question_custom_field_min_value,
            max_value::number as referral_question_custom_field_max_value,
            created_at::timestamp as referral_question_custom_field_created_at,
            updated_at::timestamp as referral_question_custom_field_updated_at

        from source

    )

select *
from renamed
