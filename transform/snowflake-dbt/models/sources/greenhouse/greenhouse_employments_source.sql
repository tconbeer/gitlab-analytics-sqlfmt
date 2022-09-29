with
    source as (select * from {{ source("greenhouse", "employments") }}),
    renamed as (

        select
            -- key
            candidate_id::number as candidate_id,

            -- info
            company_name::varchar as candidate_company_name,
            title::varchar as candidate_employment_title,
            "start"::date as candidate_employment_start_date,
            end::date as candidate_employment_end_date,
            latest::boolean as is_candidate_latest_employment,
            created_at::timestamp as candidate_employement_created_at,
            updated_at::timestamp as candidate_employement_updated_at

        from source

    )

select *
from renamed
