{{
    config(
        {
            "schema": "sensitive",
            "database": env_var("SNOWFLAKE_PREP_DATABASE"),
        }
    )
}}

with
    source as (select * from {{ source("sheetload", "ic_efficiency_competency") }}),
    renamed as (

        select
            "Timestamp"::timestamp::date as completed_date,
            "Score" as score,
            "First_Name" || ' ' || "Last_Name" as submitter_name,
            "Email_Address"::varchar as submitter_email,
            "_UPDATED_AT" as last_updated_at
        from source

    )

select *
from renamed
