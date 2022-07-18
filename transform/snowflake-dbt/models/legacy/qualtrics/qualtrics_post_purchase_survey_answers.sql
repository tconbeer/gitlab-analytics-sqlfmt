with
    responses as (

        select * from {{ ref("qualtrics_post_purchase_survey_responses_source") }}

    ),
    questions as (

        select
            *,
            ifnull(
                answer_choices[0]['1']['TextEntry'] = 'on',
                ifnull(array_size(answer_choices) = 0, true)
            ) as is_free_text
        from {{ ref("qualtrics_question") }}

    ),
    revised_question_ids as (

        select
            question_description,
            answer_choices,
            iff(is_free_text, question_id || '_TEXT', question_id) as question_id
        from questions

    ),
    parsed_out_qas as (

        select
            response_id,
            question_id,
            question_description,
            get(response_values, question_id) as question_response,
            answer_choices[0] as answer_choices,
            response_values['distributionChannel']::varchar as distribution_channel,
            iff(response_values['finished'] = 1, true, false) as has_finished_survey,
            response_values['startDate']::timestamp as survey_start_date,
            response_values['endDate']::timestamp as survey_end_date,
            response_values['recordedDate']::timestamp as response_recorded_at,
            response_values['userLanguage']::varchar as user_language,
            get(response_values, 'plan')::varchar as user_plan
        from revised_question_ids
        inner join responses on get(response_values, question_id) is not null
        where question_id != 'QID4_TEXT' and array_size(question_response) > 0

    ),
    final as (

        select
            get(answer_choices, d.value)['Display']::varchar as answer_display,
            d.value::varchar || question_id as answer_id,
            distribution_channel,
            has_finished_survey,
            response_id,
            question_description,
            question_id,
            response_recorded_at,
            survey_end_date,
            survey_start_date,
            user_language,
            user_plan
        from parsed_out_qas, lateral flatten(input => question_response) d
        where answer_display is not null


    )

select *
from final
