with
    source as (select * from {{ ref("gitlab_dotcom_emails_dedupe_source") }}),
    renamed as (

        select
            confirmation_sent_at as confirmation_sent_at,
            created_at as created_at,
            email as email_address,
            confirmed_at as email_confirmed_at,
            id as gitlab_email_id,
            user_id as user_id,
            updated_at as updated_at
        from source

    )

select *
from renamed
