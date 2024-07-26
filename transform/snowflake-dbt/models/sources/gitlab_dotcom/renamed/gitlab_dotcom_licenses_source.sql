with
    source as (select * from {{ ref("gitlab_dotcom_licenses_dedupe_source") }}),
    renamed as (

        select

            id::number as license_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at

        from source

    )

select *
from renamed
