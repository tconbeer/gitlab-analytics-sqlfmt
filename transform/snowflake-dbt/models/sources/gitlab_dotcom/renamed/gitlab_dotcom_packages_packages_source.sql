
with
    source as (

        select * from {{ ref("gitlab_dotcom_packages_packages_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as packages_package_id,
            name::varchar as package_name,
            project_id::number as project_id,
            creator_id::number as creator_id,
            version::varchar as package_version,
            package_type::varchar as package_type,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at
        from source

    )

select *
from renamed
