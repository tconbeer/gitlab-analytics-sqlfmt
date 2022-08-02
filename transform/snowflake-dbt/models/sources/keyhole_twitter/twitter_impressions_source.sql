with
    source as (select * from {{ source("keyhole_twitter", "impressions") }}),
    renamed as (

        select
            field::timestamp as impression_month,
            value::int as impressions,
            _updated_at::float as updated_at
        from source

    )

select *
from renamed
