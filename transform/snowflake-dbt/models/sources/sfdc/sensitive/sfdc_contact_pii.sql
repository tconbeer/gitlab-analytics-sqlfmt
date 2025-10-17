with
    source as (select * from {{ ref("sfdc_contact_source") }}),
    sfdc_contact_pii as (

        select
            contact_id,
            {{ nohash_sensitive_columns("sfdc_contact_source", "contact_email") }}
        from source

    )

select *
from sfdc_contact_pii
