with
    source as (select * from {{ ref("netsuite_subsidiaries_source") }}),
    entity_pii as (

        select
            subsidiary_id,
            {{
                nohash_sensitive_columns(
                    "netsuite_subsidiaries_source", "subsidiary_name"
                )
            }}
        from source

    )

select *
from entity_pii
