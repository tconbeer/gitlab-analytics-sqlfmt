with
    source as (select * from {{ source("engineering", "advisory_data") }}),
    renamed as (

        select
            "FILE"::varchar as filename,
            pubdate::date as publish_date,
            mergedate::date as merge_date,
            delta::number,
            packagetype::varchar as package_type
        from source

    )

select *
from renamed
