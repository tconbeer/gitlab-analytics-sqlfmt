with
    source as (select * from {{ ref("netsuite_vendors_source") }}),
    entity_pii as (

        select
            vendor_id,
            {{ nohash_sensitive_columns("netsuite_vendors_source", "vendor_name") }}
        from source

    )

select *
from entity_pii
