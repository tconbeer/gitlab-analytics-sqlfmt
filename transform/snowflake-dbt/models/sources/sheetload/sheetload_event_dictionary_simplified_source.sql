with
    source as (select * from {{ source("sheetload", "event_dictionary_simplified") }}),
    renamed as (

        select
            "Metric_Name"::varchar as metric_name,
            "Product_Owner"::varchar as product_owner,
            "Product_Category"::varchar as product_category,
            "Stage_Lookup"::varchar as stage_lookup
        from source

    )

select *
from renamed
