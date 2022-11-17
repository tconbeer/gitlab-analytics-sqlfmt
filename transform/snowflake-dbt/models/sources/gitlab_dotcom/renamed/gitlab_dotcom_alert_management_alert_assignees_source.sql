with
    source as (

        select *
        from {{ ref("gitlab_dotcom_alert_management_alert_assignees_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as alert_management_alert_assignee_id,
            user_id::number as user_id,
            alert_id::number as alert_id

        from source

    )

select *
from renamed
