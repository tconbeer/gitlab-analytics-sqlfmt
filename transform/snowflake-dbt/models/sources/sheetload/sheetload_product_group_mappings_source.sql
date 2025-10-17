with
    source as (select * from {{ source("sheetload", "product_group_mappings") }}),

    renamed as (
        select
            group_name::varchar as group_name,
            stage_name::varchar as stage_name,
            section_name::varchar as section_name
        from source
    )

select *
from renamed
