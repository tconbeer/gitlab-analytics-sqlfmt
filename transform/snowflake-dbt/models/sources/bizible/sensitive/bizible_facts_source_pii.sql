with
    source as (

        select {{ nohash_sensitive_columns("bizible_facts_source", "cost_key") }}
        from {{ ref("bizible_facts_source") }}

    )

select *
from source
