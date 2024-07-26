with
    source as (

        select {{ hash_sensitive_columns("xactly_pos_relations_source") }}
        from {{ ref("xactly_pos_relations_source") }}

    )

select *
from source
