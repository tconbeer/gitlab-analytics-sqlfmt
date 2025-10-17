with
    source as (

        select {{ hash_sensitive_columns("bizible_contacts_source") }}
        from {{ ref("bizible_contacts_source") }}

    )

select *
from source
