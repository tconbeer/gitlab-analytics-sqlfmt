with
    source as (

        select * from {{ ref("driveload_financial_metrics_program_phase_1_source") }}

    )

select *
from source
