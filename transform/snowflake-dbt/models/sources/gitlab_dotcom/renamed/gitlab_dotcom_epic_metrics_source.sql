with
    source as (

        select
            *,
            row_number() over (
                partition by epic_id order by updated_at desc
            ) as rank_in_key
        from {{ ref("gitlab_dotcom_epic_metrics_dedupe_source") }}

    ),
    renamed as (

        select
            epic_id::number as epic_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at

        from source

    )

select *
from renamed
