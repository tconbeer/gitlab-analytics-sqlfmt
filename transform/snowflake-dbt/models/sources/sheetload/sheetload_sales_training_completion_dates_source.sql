with
    source as (

        select * from {{ source("sheetload", "sales_training_completion_dates") }}

    )

select
    email::varchar as email_address,
    role::varchar as job_role,
    job_title::varchar as job_title,
    department::varchar as department,
    reporting_to::varchar as reporting_to,
    training::varchar as training_completed,
    knowledge_check_complete::date as knowledge_check_complete,
    application_exercise_complete::date as application_exercise_compled_date,
    _updated_at::float as last_updated_at
from source
