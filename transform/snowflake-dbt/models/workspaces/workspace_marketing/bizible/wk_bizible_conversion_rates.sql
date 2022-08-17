with
    source as (

        select {{ hash_sensitive_columns("bizible_conversion_rates_source") }}
        from {{ ref("bizible_conversion_rates_source") }}

    )

select *
from source
