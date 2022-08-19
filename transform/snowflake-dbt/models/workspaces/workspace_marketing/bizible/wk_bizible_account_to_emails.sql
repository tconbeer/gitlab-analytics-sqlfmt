with
    source as (

        select {{ hash_sensitive_columns("bizible_account_to_emails_source") }}
        from {{ ref("bizible_account_to_emails_source") }}

    )

select *
from source
