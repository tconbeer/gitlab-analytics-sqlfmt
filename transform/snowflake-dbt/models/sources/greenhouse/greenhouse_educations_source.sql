with
    source as (select * from {{ source("greenhouse", "educations") }}),
    renamed as (

        select

            -- key
            candidate_id::number as candidate_id,

            -- info
            school_name::varchar as candidate_school_name,
            degree::varchar as candidate_degree,
            discipline::varchar as candidate_discipline,
            "start"::date as candidate_education_start_date,
            end::date as candidate_education_end_date,
            latest::boolean as candidate_latest_education,
            created_at::timestamp as candidate_education_created_at,
            updated_at::timestamp as candidate_education_updated_at


        from source

    )

select *
from renamed
