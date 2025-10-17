with
    source as (

        select {{ hash_sensitive_columns("xactly_quota_assignment_hist_source") }}
        from {{ ref("xactly_quota_assignment_hist_source") }}

    )

select *
from source
