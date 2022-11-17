with
    source as (select * from {{ source("sheetload", "yc_companies") }}),
    renamed as (

        select
            md5("Name"::varchar || "Batch"::varchar) as company_id,
            "Name"::varchar as company_name,
            "Batch"::varchar as yc_batch,
            "Description"::varchar as company_description
        from source

    )

select *
from renamed
