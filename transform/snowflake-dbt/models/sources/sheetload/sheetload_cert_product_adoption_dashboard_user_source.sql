with
    source as (

        select * from {{ source("sheetload", "cert_product_adoption_dashboard_user") }}

    )

select *
from source
