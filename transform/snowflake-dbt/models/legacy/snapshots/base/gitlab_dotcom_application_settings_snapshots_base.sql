{{ config({"alias": "gitlab_dotcom_application_settings_snapshots"}) }}

with
    source as (

        select *
        from {{ source("snapshots", "gitlab_dotcom_application_settings_snapshots") }}

    ),
    renamed as (

        select
            dbt_scd_id::varchar as application_settings_snapshot_id,
            dbt_valid_from::timestamp as valid_from,
            dbt_valid_to::timestamp as valid_to,
            id::number as application_settings_id,
            shared_runners_minutes::number as shared_runners_minutes,
            repository_size_limit::number as repository_size_limit
        from source

    )

select *
from renamed
