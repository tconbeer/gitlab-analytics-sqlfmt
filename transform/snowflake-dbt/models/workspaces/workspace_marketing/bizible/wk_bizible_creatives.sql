with
    source as (

        select {{ hash_sensitive_columns("bizible_creatives_source") }}
        from {{ ref("bizible_creatives_source") }}

    )

select *
from source
