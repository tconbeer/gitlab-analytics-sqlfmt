{{ config({"materialized": "table", "schema": "legacy"}) }}

with
    greenhouse_candidate_surveys as (

        select * from {{ ref("greenhouse_candidate_surveys_source") }}

    ),
    interview_results as (

        select
            candidate_survey_id,
            organization_id,
            department_id,
            department_name,
            date_trunc('month', candidate_survey_submitted_at)::date as submitted_at,
            candidate_survey_question_1,
            case
                when candidate_survey_question_1 = 'Strongly Disagree'
                then 1
                when candidate_survey_question_1 = 'Disagree'
                then 2
                when candidate_survey_question_1 = 'Neutral'
                then 3
                when candidate_survey_question_1 = 'Agree'
                then 4
                when candidate_survey_question_1 = 'Strongly Agree'
                then 5
                else null
            end as isat_score
        from greenhouse_candidate_surveys
        where isat_score is not null {{ dbt_utils.group_by(n=7) }}

    )

select *
from interview_results
