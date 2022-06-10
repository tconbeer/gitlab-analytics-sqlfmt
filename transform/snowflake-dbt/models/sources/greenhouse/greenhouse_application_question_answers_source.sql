with
    source as (

        select * from {{ source("greenhouse", "application_question_answers") }}

    ),
    renamed as (

        select

            -- keys
            job_post_id::number as job_post_id,
            application_id::number as application_id,

            -- info
            question::varchar as application_question,
            answer::varchar as application_answer,

            created_at::timestamp as application_question_answer_created_at,
            updated_at::timestamp as application_question_answer_updated_at

        from source

    )

select *
from renamed
