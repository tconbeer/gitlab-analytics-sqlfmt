with
    source as (

        select *
        from {{ ref("sheetload_engineering_speciality_prior_to_capture_source") }}

    )

select
    employee_id,
    speciality,
    start_date as speciality_start_date,
    dateadd('day', -1, end_date) as speciality_end_date
from source
