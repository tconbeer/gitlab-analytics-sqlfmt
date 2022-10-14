with
    source as (

        select {{ hash_sensitive_columns("xactly_quota_assignment_source") }}
        from {{ ref("xactly_quota_assignment_source") }}

    )

select *
from source
