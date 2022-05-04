with
    source as (

        select * from {{ ref("gitlab_dotcom_issuable_severities_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as issue_severity_id,
            issue_id::number as issue_id,
            severity::number as severity

        from source

    )


select *
from renamed
