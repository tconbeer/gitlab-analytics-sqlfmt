with
    source as (

        select {{ hash_sensitive_columns("bizible_user_touchpoints_source") }}
        from {{ ref("bizible_user_touchpoints_source") }}

    )

select *
from source
