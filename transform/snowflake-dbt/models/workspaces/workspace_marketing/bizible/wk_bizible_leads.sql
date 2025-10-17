with
    source as (

        select {{ hash_sensitive_columns("bizible_leads_source") }}
        from {{ ref("bizible_leads_source") }}

    )

select *
from source
