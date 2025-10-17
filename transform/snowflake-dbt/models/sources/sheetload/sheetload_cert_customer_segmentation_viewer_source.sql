with
    source as (

        select * from {{ source("sheetload", "cert_customer_segmentation_viewer") }}

    )

select *
from source
