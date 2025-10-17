with
    source as (

        select {{ nohash_sensitive_columns("bizible_contacts_source", "contact_id") }}
        from {{ ref("bizible_contacts_source") }}

    )

select *
from source
