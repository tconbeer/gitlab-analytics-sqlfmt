with
    saml_providers as (select * from {{ ref("gitlab_dotcom_saml_providers") }}),
    identities as (select * from {{ ref("gitlab_dotcom_identities") }}),
    joined as (

        select
            saml_providers.*,
            count(distinct user_id) as saml_provider_user_count,
            min(created_at) as first_saml_provider_created_at
        from saml_providers
        left join
            identities
            on saml_providers.saml_provider_id = identities.saml_provider_id
            and {{ filter_out_blocked_users("identities", "user_id") }}
            {{ dbt_utils.group_by(n=9) }}
    )

select *
from joined
