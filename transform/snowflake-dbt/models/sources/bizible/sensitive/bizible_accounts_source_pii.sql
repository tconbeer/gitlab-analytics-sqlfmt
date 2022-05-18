with
    source as (

        select {{ nohash_sensitive_columns("bizible_accounts_source", "account_id") }}
        from {{ ref("bizible_accounts_source") }}

    )

select *
from source
