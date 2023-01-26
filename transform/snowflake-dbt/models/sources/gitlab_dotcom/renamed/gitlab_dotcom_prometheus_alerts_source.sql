with
    source as (

        select * from {{ ref("gitlab_dotcom_prometheus_alerts_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as prometheus_alert_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            project_id::number as project_id
        from source

    )

select *
from renamed
