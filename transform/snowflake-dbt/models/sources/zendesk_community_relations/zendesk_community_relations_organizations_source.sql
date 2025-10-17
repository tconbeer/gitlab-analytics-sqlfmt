with
    source as (

        select * from {{ source("zendesk_community_relations", "organizations") }}

    ),

    renamed as (

        select

            -- ids
            id as organization_id,

            -- fields
            name as organization_name,
            tags as organization_tags,

            -- dates
            created_at,
            updated_at

        from source

    )

select *
from renamed
