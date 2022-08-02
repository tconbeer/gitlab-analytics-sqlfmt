with
    source as (

        select {{ hash_sensitive_columns("sfdc_contact_source") }}
        from {{ ref("sfdc_contact_source") }}
        where is_deleted = false

    )

select *
from source
