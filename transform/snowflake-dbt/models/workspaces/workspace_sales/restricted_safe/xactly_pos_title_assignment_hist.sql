with
    source as (

        select {{ hash_sensitive_columns("xactly_pos_title_assignment_hist_source") }}
        from {{ ref("xactly_pos_title_assignment_hist_source") }}

    )

select *
from source
