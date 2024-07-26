with
    source as (

        select {{ hash_sensitive_columns("bizible_keywords_source") }}
        from {{ ref("bizible_keywords_source") }}

    )

select *
from source
