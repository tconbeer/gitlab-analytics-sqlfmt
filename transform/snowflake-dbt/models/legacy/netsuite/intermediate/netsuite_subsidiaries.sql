with
    source as (

        select {{ hash_sensitive_columns("netsuite_subsidiaries_source") }}
        from {{ ref("netsuite_subsidiaries_source") }}

    )

select *
from source
