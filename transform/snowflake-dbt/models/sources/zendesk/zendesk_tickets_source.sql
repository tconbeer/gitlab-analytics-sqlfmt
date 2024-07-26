with
    source as (select * from {{ source("zendesk", "tickets") }}),

    renamed as (

        select
            -- ids
            id as ticket_id,
            organization_id,
            assignee_id,
            brand_id,
            group_id,
            requester_id,
            submitter_id,
            ticket_form_id::number as ticket_form_id,

            -- fields
            status as ticket_status,
            lower(priority) as ticket_priority,
            md5(subject) as ticket_subject,
            md5(recipient) as ticket_recipient,
            url as ticket_url,
            tags as ticket_tags,
            -- added ':score'
            satisfaction_rating__id::varchar as satisfaction_rating_id,
            satisfaction_rating__score::varchar as satisfaction_rating_score,
            via__channel::varchar as submission_channel,
            custom_fields::array as ticket_custom_field_values,

            -- dates
            updated_at::date as date_updated,
            created_at as ticket_created_at

        from source

    )

select *
from renamed
