with
    source as (

        select *
        from {{ ref("gitlab_dotcom_design_management_designs_versions_dedupe_source") }}

    ),
    renamed as (

        select
            md5(id) as design_version_id,
            design_id::varchar as design_id,
            version_id::number as version_id,
            event::number as event_type_id
        from source

    )

select *
from renamed
