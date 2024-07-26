with
    source as (

        select {{ hash_sensitive_columns("gitlab_contact_enhance_source") }}
        from {{ ref("gitlab_contact_enhance_source") }}

    )

select *
from source
