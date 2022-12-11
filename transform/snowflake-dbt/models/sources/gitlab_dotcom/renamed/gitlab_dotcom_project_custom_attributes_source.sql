with
    source as (

        select * from {{ ref("gitlab_dotcom_project_custom_attributes_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as project_custom_attribute_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            project_id::number as project_id,
            key::varchar as project_custom_key,
            value::varchar as project_custom_value
        from source

    )

select *
from renamed
