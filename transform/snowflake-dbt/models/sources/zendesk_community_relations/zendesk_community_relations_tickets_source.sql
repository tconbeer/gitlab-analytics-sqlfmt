with
    source as (select * from {{ source("zendesk_community_relations", "tickets") }}),

    renamed as (

        select
            id as ticket_id,
            created_at as ticket_created_at,
            -- ids
            organization_id,
            assignee_id,
            brand_id,
            group_id,
            requester_id,
            submitter_id,

            -- fields
            status as ticket_status,
            lower(priority) as ticket_priority,
            md5(subject) as ticket_subject,
            md5(recipient) as ticket_recipient,
            url as ticket_url,
            tags as ticket_tags,
            -- added ':score'
            -- satisfaction_rating['score']::VARCHAR   AS satisfaction_rating_score,
            via__channel::varchar as submission_channel,

            -- dates
            updated_at::date as date_updated

        from source

    )

select *
from renamed
