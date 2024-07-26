with
    source as (select * from {{ ref("gitlab_dotcom_label_priorities_dedupe_source") }}),
    renamed as (

        select

            id::number as label_priority_id,
            project_id::number as project_id,
            label_id::number as label_id,
            priority::number as priority,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at

        from source

    )

select *
from renamed
