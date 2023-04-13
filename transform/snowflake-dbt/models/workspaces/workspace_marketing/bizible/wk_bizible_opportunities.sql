with
    source as (

        select {{ hash_sensitive_columns("bizible_opportunities_source") }}
        from {{ ref("bizible_opportunities_source") }}

    )

select *
from source
