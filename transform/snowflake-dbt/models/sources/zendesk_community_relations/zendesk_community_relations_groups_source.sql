with
    source as (select * from {{ source("zendesk_community_relations", "groups") }}),

    renamed as (

        select

            -- ids
            id as group_id,

            -- field
            url as group_url,
            name as group_name,
            deleted as is_deleted,

            -- dates
            created_at,
            updated_at

        from source

    )

select *
from renamed
