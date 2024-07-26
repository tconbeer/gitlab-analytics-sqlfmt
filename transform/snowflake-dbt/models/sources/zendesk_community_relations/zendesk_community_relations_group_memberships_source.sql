with
    source as (

        select * from {{ source("zendesk_community_relations", "group_memberships") }}

    ),

    renamed as (

        select

            -- ids
            id as group_membership_id,
            group_id as group_id,
            user_id as user_id,

            -- field
            "DEFAULT" as is_default_group_membership,
            url as group_membership_url,

            -- dates
            created_at,
            updated_at

        from source

    )

select *
from renamed
