with
    source as (

        select *
        from {{ ref("sheetload_engineering_infra_prod_console_access_source") }}

    )

select *
from source
