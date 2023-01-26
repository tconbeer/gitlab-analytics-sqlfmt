with
    source as (select * from {{ source("demandbase", "account_site_page_metrics") }}),
    renamed as (

        select
            jsontext['account_id']::number as account_id,
            jsontext['base_page']::varchar as base_page,
            jsontext['date']::date as metric_date,
            jsontext['page_view_count']::number as page_view_count,
            jsontext['partition_date']::date as partition_date
        from source
        where
            partition_date = (select max(jsontext['partition_date']::date) from source)

    )

select *
from renamed
