with
    source as (

        select * from {{ ref("gitlab_dotcom_application_settings_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as application_settings_id,
            shared_runners_minutes::number as shared_runners_minutes
        from source

    )

select *
from renamed
