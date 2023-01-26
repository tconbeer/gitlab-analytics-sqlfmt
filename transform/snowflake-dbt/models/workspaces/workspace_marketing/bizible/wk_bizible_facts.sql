with
    source as (

        select {{ hash_sensitive_columns("bizible_facts_source") }}
        from {{ ref("bizible_facts_source") }}

    )

select *
from source
