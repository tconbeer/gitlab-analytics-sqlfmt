with
    source as (select * from {{ source("greenhouse", "eeoc_responses") }}),
    renamed as (

        select

            -- key
            application_id::number as application_id,

            -- info
            status::varchar as candidate_status,
            race::varchar as candidate_race,
            gender::varchar as candidate_gender,
            disability_status::varchar as candidate_disability_status,
            veteran_status::varchar as candidate_veteran_status,
            submitted_at::timestamp as eeoc_response_submitted_at

        from source
        where eeoc_response_submitted_at is not null

    )

select *
from renamed
