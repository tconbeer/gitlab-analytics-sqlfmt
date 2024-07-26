with
    source as (select * from {{ source("greenhouse", "scorecard_question_answers") }}),
    renamed as (

        select

            -- keys
            scorecard_id::number as scorecard_id,
            application_id::number as application_id,

            -- info
            question::varchar as scorecard_question,
            answer::varchar as scorecard_answer,
            created_at::timestamp as scorecard_question_answer_created_at,
            updated_at::varchar::timestamp as scorecard_question_answer_updated_at

        from source

    )

select *
from renamed
