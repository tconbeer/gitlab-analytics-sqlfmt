with
    source as (
        select {{ hash_sensitive_columns("sfdc_lead_source") }}
        from {{ ref("sfdc_lead_source") }}
        where is_deleted = false
    )
select *
from source
