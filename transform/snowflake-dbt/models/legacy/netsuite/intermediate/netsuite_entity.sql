with
    source as (

        select {{ hash_sensitive_columns("netsuite_entity_source") }}
        from {{ ref("netsuite_entity_source") }}

    )

select *
from source
where is_fivetran_deleted = false
