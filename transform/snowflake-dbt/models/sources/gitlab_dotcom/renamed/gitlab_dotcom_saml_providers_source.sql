with
    source as (select * from {{ ref("gitlab_dotcom_saml_providers_dedupe_source") }}),
    renamed as (

        select
            id::number as saml_provider_id,
            group_id::number as group_id,
            enabled::boolean as is_enabled,
            certificate_fingerprint::varchar as certificate_fingerprint,
            sso_url::varchar as sso_url,
            enforced_sso::boolean as is_enforced_sso,
            enforced_group_managed_accounts::boolean
            as is_enforced_group_managed_accounts,
            prohibited_outer_forks::boolean as is_prohibited_outer_forks,
            default_membership_role::number as default_membership_role_id
        from source

    )

select *
from renamed
