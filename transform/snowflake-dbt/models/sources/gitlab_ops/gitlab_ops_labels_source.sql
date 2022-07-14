with
    source as (

        select *
        from {{ source("gitlab_ops", "labels") }}
        qualify row_number() OVER (partition by id order by updated_at desc) = 1

    ),
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
