with
    source as (select * from {{ ref("netsuite_entity_source") }}),
    entity_pii as (

        select
            entity_id,
            {{ nohash_sensitive_columns("netsuite_entity_source", "entity_name") }}
        from source

    )

select *
from entity_pii
