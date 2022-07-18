with
    source_status as (select * from {{ ref("dbt_source_freshness") }}),
    filtered_to_snapshots as (

        select distinct
            table_name, date_trunc('d', latest_load_at) as successful_load_at
        from source_status
        where lower(table_name) like '%snapshot%'
        order by 2 desc

    )

select *
from filtered_to_snapshots
