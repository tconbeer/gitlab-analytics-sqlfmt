with
    source as (

        select * from {{ ref("sheetload_manual_public_company_metrics_source") }}

    )

select *
from source
