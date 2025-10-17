with
    source as (select * from {{ ref("gitlab_dotcom_terraform_states_dedupe_source") }}),
    renamed as (

        select

            id::number as terraform_state_id,
            project_id::number as project_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            file_store::varchar as file_store

        from source

    )

select *
from renamed
