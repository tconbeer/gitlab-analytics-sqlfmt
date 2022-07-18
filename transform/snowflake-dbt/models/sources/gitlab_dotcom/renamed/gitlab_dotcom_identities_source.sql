
with
    source as (select * from {{ ref("gitlab_dotcom_identities_dedupe_source") }}),
    renamed as (

        select
            id::number as identity_id,
            extern_uid::varchar as extern_uid,
            provider::varchar as identity_provider,
            user_id::number as user_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            -- econdary_extern_uid // always null
            saml_provider_id::number as saml_provider_id
        from source

    )

select *
from renamed
