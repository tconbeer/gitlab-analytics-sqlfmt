with
    source as (select * from {{ source("qualtrics", "questions") }}),
    questions as (

        select d.value as data_by_row, uploaded_at
        from source, lateral flatten(input => parse_json(jsontext), outer => true) d

    ),
    parsed as (

        select
            data_by_row['survey_id']::varchar as survey_id,
            data_by_row['QuestionID']::varchar as question_id,
            data_by_row['QuestionDescription']::varchar as question_description,
            data_by_row['Choices']::array as answer_choices
        from questions
        qualify
            row_number() over (partition by question_id order by uploaded_at desc) = 1

    )
select *
from parsed
