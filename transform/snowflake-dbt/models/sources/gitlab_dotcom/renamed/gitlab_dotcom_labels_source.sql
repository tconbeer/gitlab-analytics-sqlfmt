with
    source as (select * from {{ ref("gitlab_dotcom_labels_dedupe_source") }}),
    renamed as (

        select

            id::number as label_id,
            title as label_title,
            color,
            source.project_id::number as project_id,
            group_id::number as group_id,
            template,
            type as label_type,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at

        from source

    )

select *
from renamed
