with
    source as (

        select *
        from {{ source("sheetload", "engineering_speciality_prior_to_capture") }}

    ),
    renamed as (

        select
            "EMPLOYEE_ID" as employee_id,
            "FULL_NAME" as full_name,
            "REPORTS_TO" as reports_to,
            "DIVISION" as division,
            "DEPARTMENT" as department,
            "JOBTITLE_SPECIALITY" as speciality,
            "EFFECTIVE_DATE"::timestamp::date as start_date,
            "END_DATE"::timestamp::date as end_date
        from source

    )

select *
from renamed
