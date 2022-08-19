with
    source as (select * from {{ source("greenhouse", "candidate_custom_fields") }}),
    renamed as (

        select
            -- keys
            candidate_id::number as candidate_id,
            user_id::number as greenhouse_user_id,

            -- info
            custom_field::varchar as candidate_custom_field,
            float_value::float as candidate_custom_field_float_value,
            date_value::varchar::date as candidate_custom_field_date,
            display_value::varchar as candidate_custom_field_display_value,
            min_value::number as candidate_custom_field_min_value,
            max_value::number as candidate_custom_field_max_value,
            created_at::timestamp as candidate_custom_field_created_at,
            updated_at::timestamp as candidate_custom_field_updated_at

        from source

    )

select *
from renamed
