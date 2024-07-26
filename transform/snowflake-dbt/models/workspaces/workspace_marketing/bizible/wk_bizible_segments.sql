with
    source as (

        select {{ hash_sensitive_columns("bizible_segments_source") }}
        from {{ ref("bizible_segments_source") }}

    )

select *
from source
