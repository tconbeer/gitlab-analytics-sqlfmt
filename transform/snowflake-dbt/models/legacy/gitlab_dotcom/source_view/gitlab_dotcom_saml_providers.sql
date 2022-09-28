with source as (select * from {{ ref("gitlab_dotcom_saml_providers_source") }})

select *
from source
