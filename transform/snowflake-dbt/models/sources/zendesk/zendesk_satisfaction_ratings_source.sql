with
    source as (select * from {{ source("zendesk", "satisfaction_ratings") }}),

    renamed as (

        select

            -- ids
            id as satisfaction_rating_id,
            assignee_id as assignee_id,
            group_id as group_id,
            reason_id as reason_id,
            requester_id as requester_id,
            ticket_id as ticket_id,

            -- field
            comment as comment,
            reason as reason,
            score,
            url as satisfaction_rating_url,


            -- dates
            created_at,
            updated_at

        from source

    )

select *
from renamed
