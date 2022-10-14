with
    source as (

        select {{ nohash_sensitive_columns("bizible_crm_tasks_source", "crm_task_id") }}
        from {{ ref("bizible_crm_tasks_source") }}

    )

select *
from source
