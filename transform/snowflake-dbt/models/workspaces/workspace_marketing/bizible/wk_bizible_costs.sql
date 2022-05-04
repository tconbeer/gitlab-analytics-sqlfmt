with
    source as (

        select {{ hash_sensitive_columns("bizible_costs_source") }}
        from {{ ref("bizible_costs_source") }}

    )

select *
from source
