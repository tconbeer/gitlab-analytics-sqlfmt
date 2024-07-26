with
    source as (

        select * from {{ source("sheetload", "percent_over_comp_band_historical") }}

    )

select *
from source
