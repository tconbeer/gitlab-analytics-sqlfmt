with
    source as (

        select
            {{
                nohash_sensitive_columns(
                    "bizible_account_to_emails_source", "account_to_email_id"
                )
            }}
        from {{ ref("bizible_account_to_emails_source") }}

    )

select *
from source
