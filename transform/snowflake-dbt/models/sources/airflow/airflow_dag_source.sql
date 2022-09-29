with
    source as (select * from {{ source("airflow", "dag") }}),
    renamed as (

        select
            dag_id::varchar as dag_id,
            is_active::boolean as is_active,
            is_paused::varchar as is_paused,
            schedule_interval::varchar as schedule_interval
        from source

    )

select *
from renamed
