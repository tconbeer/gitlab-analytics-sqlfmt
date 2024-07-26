with
    source as (

        select {{ hash_sensitive_columns("bizible_activities_source") }}
        from {{ ref("bizible_activities_source") }}

    )

select *
from source
