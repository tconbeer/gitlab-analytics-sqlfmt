with
    source as (

        select * from {{ source("sheetload", "engineering_infra_prod_console_access") }}
    )

select *
from source
