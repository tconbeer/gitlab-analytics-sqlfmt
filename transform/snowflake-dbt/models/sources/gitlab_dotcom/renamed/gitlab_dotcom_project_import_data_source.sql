with
    source as (

        select * from {{ ref("gitlab_dotcom_project_import_data_dedupe_source") }}

    ),
    renamed as (

        select

            id::number as project_import_relation_id, project_id::number as project_id

        from source

    )

select *
from renamed
