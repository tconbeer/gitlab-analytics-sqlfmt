with
    source as (select * from {{ source("greenhouse", "scorecards_attributes") }}),
    renamed as (

        select

            -- keys
            scorecard_id::number as scorecard_id,
            attribute_id::number as scorecard_attribute_id,

            -- info
            rating::varchar as scorecard_attribute_rating,
            notes::varchar as scorecard_attribute_notes,
            created_at::timestamp as scorecard_attribute_created_at,
            updated_at::timestamp as scorecard_attribute_updated_at

        from source

    )

select *
from renamed
