with
    answers as (

        select *
        from {{ ref("qualtrics_nps_answers") }}
        where question_id in ('QID172787673', 'QID172787675_TEXT')

    ),
    trimmed as (

        select
            response_id,
            distribution_channel,
            has_finished_survey,
            user_language,
            survey_start_date,
            survey_end_date,
            response_recorded_at,
            user_plan,
            max(
                iff(question_id = 'QID172787673', question_response, null)::number
            ) as nps_score,
            max(
                iff(question_id = 'QID172787675_TEXT', question_response, null)::varchar
            ) as nps_reason
        from answers {{ dbt_utils.group_by(n=8) }}

    ),
    final as (

        select
            *,
            case
                when nps_score >= 9
                then 'promoter'
                when nps_score >= 7
                then 'passive'
                when nps_score >= 0
                then 'detractor'
            end as nps_bucket_text,
            case
                when nps_bucket_text = 'promoter'
                then 100
                when nps_bucket_text = 'passive'
                then 0
                when nps_bucket_text = 'detractor'
                then -100
            end as nps_bucket_integer
        from trimmed

    )

select *
from final
order by response_recorded_at
