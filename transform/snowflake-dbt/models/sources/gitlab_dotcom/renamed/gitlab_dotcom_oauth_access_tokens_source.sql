with
    source as (

        select * from {{ ref("gitlab_dotcom_oauth_access_tokens_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as oauth_access_token_id,
            resource_owner_id::number as resource_owner_id,
            application_id::number as application_id,
            expires_in::number as expires_in_seconds,
            revoked_at::timestamp as oauth_access_token_revoked_at,
            created_at::timestamp as created_at,
            scopes::varchar as scopes
        from source

    )

select *
from renamed
order by created_at
