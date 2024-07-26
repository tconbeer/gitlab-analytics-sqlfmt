with
    source as (

        select {{ hash_sensitive_columns("netsuite_vendors_source") }}
        from {{ ref("netsuite_vendors_source") }}

    )

select *
from source
