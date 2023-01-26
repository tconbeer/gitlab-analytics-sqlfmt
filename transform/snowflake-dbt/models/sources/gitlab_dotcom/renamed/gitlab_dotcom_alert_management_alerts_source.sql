with
    source as (

        select * from {{ ref("gitlab_dotcom_alert_management_alerts_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as alert_management_alert_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            started_at::timestamp as started_at,
            ended_at::timestamp as ended_at,
            events::number as alert_management_alert_events,
            iid::number as alert_management_alert_iid,
            severity::number as severity_id,
            status::number as status_id,
            issue_id::number as issue_id,
            project_id::number as project_id,
            service::varchar as alert_management_alert_service,
            monitoring_tool::varchar as monitoring_tool

        from source

    )

select *
from renamed
