with
    source as (

        select {{ hash_sensitive_columns("bizible_advertisers_source") }}
        from {{ ref("bizible_advertisers_source") }}

    )

select *
from source
