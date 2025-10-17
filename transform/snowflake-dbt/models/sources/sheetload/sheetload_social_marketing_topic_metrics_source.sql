with
    source as (

        select * from {{ source("sheetload", "social_marketing_topic_metrics") }}

    ),
    renamed as (

        select
            month::date as month_date,
            sprout_tag::varchar as sprout_tag,
            channel::varchar as channel,
            brand::varchar as brand,
            metric::varchar as metric,
            is_organic::boolean as is_organic,
            value::number as value,
            source::varchar as source,
            source_details::varchar as source_details
        from source
    )

select *
from renamed
