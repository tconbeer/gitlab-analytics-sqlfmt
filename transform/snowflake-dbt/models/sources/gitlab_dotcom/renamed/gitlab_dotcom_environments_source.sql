
with
    source as (select * from {{ ref("gitlab_dotcom_environments_dedupe_source") }}),
    renamed as (

        select
            id::number as environment_id,
            project_id::number as project_id,
            name::varchar as environment_name,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            external_url::varchar as external_url,
            environment_type::varchar as environment_type,
            state::varchar as state,
            slug::varchar as slug
        from source

    )
select *
from renamed
