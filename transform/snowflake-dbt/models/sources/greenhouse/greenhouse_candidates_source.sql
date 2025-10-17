with
    source as (select * from {{ source("greenhouse", "candidates") }}),
    renamed as (

        select
            -- keys
            id::number as candidate_id,
            recruiter_id::number as candidate_recruiter_id,
            coordinator_id::number as candidate_coordinator_id,

            -- info
            recruiter::varchar as candidate_recruiter,
            coordinator::varchar as candidate_coordinator,
            company::varchar as candidate_company,
            title::varchar as candidate_title,
            created_at::timestamp as candidate_created_at,
            updated_at::timestamp as candidate_updated_at,
            migrated::boolean as is_candidate_migrated,
            private::boolean as is_candidate_private

        from source

    )

select *
from renamed
