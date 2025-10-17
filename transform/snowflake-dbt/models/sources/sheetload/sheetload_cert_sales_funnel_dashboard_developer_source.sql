with
    source as (

        select * from {{ source("sheetload", "cert_sales_funnel_dashboard_developer") }}

    )

select *
from source
