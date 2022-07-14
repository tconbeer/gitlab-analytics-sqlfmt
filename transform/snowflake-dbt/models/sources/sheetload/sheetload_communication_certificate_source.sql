{{
    config(
        {
            "schema": "sensitive",
            "database": env_var("SNOWFLAKE_PREP_DATABASE"),
        }
    )
}}

with
    source as (select * from {{ source("sheetload", "communication_certificate") }}),
    renamed as (

        select
            "Timestamp"::timestamp::date as completed_date,
            "Score" as score,
            "First_&_Last_Name" as submitter_name,
            "Email_address_(GitLab_team_members,_please_use_your_GitLab_email_address)"
            ::varchar as submitter_email,
            "_UPDATED_AT" as last_updated_at
        from source

    )

select *
from renamed
