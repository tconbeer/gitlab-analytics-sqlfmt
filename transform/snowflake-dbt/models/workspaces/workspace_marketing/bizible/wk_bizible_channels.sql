with
    source as (

        select {{ hash_sensitive_columns("bizible_channels_source") }}
        from {{ ref("bizible_channels_source") }}

    )

select *
from source
