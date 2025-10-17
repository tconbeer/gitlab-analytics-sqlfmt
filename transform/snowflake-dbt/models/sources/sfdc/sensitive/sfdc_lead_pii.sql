with
    source as (select * from {{ ref("sfdc_lead_source") }}),
    sfdc_lead_pii as (

        select lead_id, {{ nohash_sensitive_columns("sfdc_lead_source", "lead_email") }}
        from source

    )

select *
from sfdc_lead_pii
