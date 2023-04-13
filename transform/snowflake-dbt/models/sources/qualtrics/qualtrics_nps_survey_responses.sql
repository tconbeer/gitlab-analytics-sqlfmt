{{
    config(
        {
            "schema": "sensitive",
            "database": env_var("SNOWFLAKE_PREP_DATABASE"),
        }
    )
}}

with
    source as (select * from {{ source("qualtrics", "nps_survey_responses") }}),
    parsed as (

        select d.value as data_by_row, uploaded_at
        from
            source,
            lateral flatten(input => parse_json(jsontext['responses']), outer => true) d

    ),
    response_parsed as (

        select
            data_by_row['responseId']::varchar as response_id,
            data_by_row['values']::variant as response_values
        from parsed
        qualify
            row_number() over (partition by response_id order by uploaded_at desc) = 1

    )
select *
from response_parsed
