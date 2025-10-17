with
    source as (

        select * from {{ source("sheetload", "cert_pricing_customer_discount_viewer") }}

    )

select *
from source
