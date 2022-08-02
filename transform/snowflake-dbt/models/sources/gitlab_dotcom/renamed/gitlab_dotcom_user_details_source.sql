with
    source as (select * from {{ ref("gitlab_dotcom_user_details_dedupe_source") }}),
    renamed as (

        select
            user_id::number as user_id,
            job_title::varchar as job_title,
            other_role::varchar as other_role,
            registration_objective::number as registration_objective
        from source

    )

select *
from renamed
