with
    source as (select * from {{ source("greenhouse", "candidate_survey_questions") }}),
    renamed as (

        select
            -- keys
            id::number as candidate_survey_question_id,

            -- info
            question::varchar as candidate_survey_question

        from source

    )

select *
from renamed
