with
    source as (select * from {{ source("sheetload", "territory_mapping") }}),
    renamed as (

        select
            "Segment"::varchar as segment,
            "Region"::varchar as region,
            "Sub_Region"::varchar as sub_region,
            "Area"::varchar as area,
            "Territory"::varchar as territory
        from source

    )

select *
from renamed
