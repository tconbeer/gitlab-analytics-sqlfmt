with
    source as (

        select {{ hash_sensitive_columns("bizible_sites_source") }}
        from {{ ref("bizible_sites_source") }}

    )

select *
from source
