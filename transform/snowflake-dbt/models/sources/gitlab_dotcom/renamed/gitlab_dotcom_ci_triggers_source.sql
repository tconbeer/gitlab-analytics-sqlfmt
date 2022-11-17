
with
    source as (select * from {{ ref("gitlab_dotcom_ci_triggers_dedupe_source") }}),
    renamed as (

        select

            id::number as ci_trigger_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            project_id::number as project_id,
            owner_id::number as owner_id,
            description::varchar as ci_trigger_description

        from source

    )

select *
from renamed
