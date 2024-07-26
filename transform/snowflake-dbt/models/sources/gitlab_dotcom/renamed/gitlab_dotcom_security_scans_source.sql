with
    source as (select * from {{ ref("gitlab_dotcom_security_scans_dedupe_source") }}),

    renamed as (

        select
            id::number as security_scan_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            build_id::number as build_id,
            scan_type::number as scan_type,
            project_id::number as project_id,
            pipeline_id::number as pipeline_id,
            latest::boolean as is_latest,
            status::number as security_scan_status
        from source

    )

select *
from renamed
