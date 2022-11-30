with
    source as (

        select {{ hash_sensitive_columns("xactly_pos_part_assignment_source") }}
        from {{ ref("xactly_pos_part_assignment_source") }}

    )

select *
from source
