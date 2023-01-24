with
    source as (select * from {{ source("airflow", "dag_run") }}),
    renamed as (

        select
            dag_id::varchar as dag_id,
            execution_date::timestamp as execution_date,
            state::varchar as run_state
        from source

    )

select *
from renamed
