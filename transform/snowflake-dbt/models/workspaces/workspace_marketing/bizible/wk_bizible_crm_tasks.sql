with
    source as (

        select {{ hash_sensitive_columns("bizible_crm_tasks_source") }}
        from {{ ref("bizible_crm_tasks_source") }}

    )

select *
from source
