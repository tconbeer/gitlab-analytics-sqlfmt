with
    source as (select * from {{ source("sheetload", "fmm_kpi_targets") }}),
    renamed as (

        select
            field_segment::varchar as field_segment,
            region::varchar as region,
            kpi::varchar as kpi,
            goal::number as goal
        from source

    )

select *
from renamed
