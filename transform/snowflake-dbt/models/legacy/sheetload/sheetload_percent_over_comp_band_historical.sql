with
    source as (

        select * from {{ ref("sheetload_percent_over_comp_band_historical_source") }}

    )

select *
from source
