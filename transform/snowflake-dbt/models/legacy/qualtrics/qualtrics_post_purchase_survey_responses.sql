{{ simple_cte([("survey_source", "qualtrics_post_purchase_survey_responses_source")]) }},
parsed_json as (

    select
        iff(
            response_values['finished']::number = 1, true, false
        )::boolean as is_finished,
        response_values['plan']::varchar as user_plan,
        response_values['account_id']::varchar as account_id,
        response_values['recordedDate']::timestamp as recorded_at,
        response_values['startDate']::timestamp as started_at,
        response_id
    from survey_source
)

select *
from parsed_json
order by recorded_at desc
