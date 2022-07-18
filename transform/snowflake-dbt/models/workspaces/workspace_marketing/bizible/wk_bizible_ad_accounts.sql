with
    source as (

        select {{ hash_sensitive_columns("bizible_ad_accounts_source") }}
        from {{ ref("bizible_ad_accounts_source") }}

    )

select *
from source
