with
    source as (

        select {{ hash_sensitive_columns("xactly_credit_source") }}
        from {{ ref("xactly_credit_source") }}

    )

select *
from source
