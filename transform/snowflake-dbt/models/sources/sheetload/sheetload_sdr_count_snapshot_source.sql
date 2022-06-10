with
    source as (select * from {{ source("sheetload", "sdr_count_snapshot") }}),
    renamed as (

        select
            "Quarter"::varchar as fiscal_quarter,
            "Sales_Segment"::varchar as sales_segment,
            "SDR_Count"::numeric as sdr_count

        from source
    )

select *
from renamed
