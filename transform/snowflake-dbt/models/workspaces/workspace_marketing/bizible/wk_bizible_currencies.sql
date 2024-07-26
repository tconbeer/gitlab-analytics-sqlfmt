with
    source as (

        select {{ hash_sensitive_columns("bizible_currencies_source") }}
        from {{ ref("bizible_currencies_source") }}

    )

select *
from source
