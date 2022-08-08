with
    source as (

        select {{ hash_sensitive_columns("bizible_form_submits_source") }}
        from {{ ref("bizible_form_submits_source") }}

    )

select *
from source
