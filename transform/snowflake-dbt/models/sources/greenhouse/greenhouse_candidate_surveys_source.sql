with
    source as (select * from {{ source("greenhouse", "candidate_surveys") }}),
    renamed as (

        select

            -- keys
            id::number as candidate_survey_id,
            organization_id::number as organization_id,
            department_id::number as department_id,
            office_id::number as office_id,

            -- info
            department_name::varchar as department_name,
            office_name::varchar as office_name,
            question_1::varchar as candidate_survey_question_1,
            question_2::varchar as candidate_survey_question_2,
            question_3::varchar as candidate_survey_question_3,
            question_4::varchar as candidate_survey_question_4,
            question_5::varchar as candidate_survey_question_5,
            question_6::varchar as candidate_survey_question_6,
            question_7::varchar as candidate_survey_question_7,
            question_8::varchar as candidate_survey_question_8,
            submitted_at::timestamp as candidate_survey_submitted_at

        from source

    )

select *
from renamed
