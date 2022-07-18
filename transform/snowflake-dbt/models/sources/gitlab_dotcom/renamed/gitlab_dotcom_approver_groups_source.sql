with
    source as (select * from {{ ref("gitlab_dotcom_approver_groups_dedupe_source") }}),
    renamed as (

        select
            id::number as approver_group_id,
            target_type::varchar as target_type,
            group_id::number as group_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at

        from source

    )


select *
from renamed
