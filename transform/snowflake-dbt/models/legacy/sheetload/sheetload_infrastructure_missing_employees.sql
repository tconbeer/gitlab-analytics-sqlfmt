with
    source as (

        select * from {{ ref("sheetload_infrastructure_missing_employees_source") }}

    )

select *
from source
