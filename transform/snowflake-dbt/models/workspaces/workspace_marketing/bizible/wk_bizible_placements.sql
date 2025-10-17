with
    source as (

        select {{ hash_sensitive_columns("bizible_placements_source") }}
        from {{ ref("bizible_placements_source") }}

    )

select *
from source
