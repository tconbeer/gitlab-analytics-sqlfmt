with
    source as (select * from {{ ref("gitlab_dotcom_dast_profiles_dedupe_source") }}),
    renamed as (

        select
            id::number as dast_profiles_id,
            project_id::number as project_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at
        from source

    )

select *
from renamed
