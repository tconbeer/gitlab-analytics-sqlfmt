with
    source as (

        select {{ hash_sensitive_columns("bizible_stage_definitions_source") }}
        from {{ ref("bizible_stage_definitions_source") }}

    )

select *
from source
