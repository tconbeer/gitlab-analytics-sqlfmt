with
    source as (select * from {{ ref("airflow_dag_source") }}),
    renamed as (select dag_id, is_active, is_paused, schedule_interval from source)

select *
from renamed
