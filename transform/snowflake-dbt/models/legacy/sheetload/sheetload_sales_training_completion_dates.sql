with
    source as (

        select * from {{ ref("sheetload_sales_training_completion_dates_source") }}

    )

select *
from source
