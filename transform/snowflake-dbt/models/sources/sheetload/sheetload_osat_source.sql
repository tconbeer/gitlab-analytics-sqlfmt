with
    source as (select * from {{ source("sheetload", "osat") }}),
    renamed as (

        select
            try_to_timestamp_ntz("TIMESTAMP")::date as completed_date,
            "EMPLOYEE_NAME"::varchar as employee_name,
            "DIVISION"::varchar as division,
            nullif("SATISFACTION_SCORE", '')::number as satisfaction_score,
            nullif("RECOMMEND_TO_FRIEND", '')::number as recommend_to_friend,
            nullif(
                onboarding_buddy_experience_score, ''
            )::number as buddy_experience_score,
            try_to_timestamp_ntz("HIRE_DATE")::date as hire_date

        from source

    )

select *
from renamed
