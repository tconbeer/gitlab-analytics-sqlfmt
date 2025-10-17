with
    source as (select * from {{ ref("gitlab_dotcom_vulnerabilities_dedupe_source") }}),
    renamed as (

        select
            id::number as vulnerability_id,
            confidence::number as confidence,
            confidence_overridden::boolean as is_confidence_overridden,
            confirmed_at::timestamp as confirmed_at,
            created_at::timestamp as created_at,
            dismissed_at::timestamp as dismissed_at,
            resolved_at::timestamp as resolved_at,
            severity_overridden::boolean as is_severity_overriden,
            state::number as state,
            updated_at::timestamp as updated_at
        from source

    )

select *
from renamed
