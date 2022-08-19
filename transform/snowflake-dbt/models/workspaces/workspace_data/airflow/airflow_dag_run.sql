with
    source as (select * from {{ ref("airflow_dag_run_source") }}),
    renamed as (select dag_id, execution_date, run_state from source)

select *
from renamed
