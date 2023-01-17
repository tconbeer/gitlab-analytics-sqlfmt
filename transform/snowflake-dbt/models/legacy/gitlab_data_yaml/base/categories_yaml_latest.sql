with
    source as (select * from {{ ref("categories_yaml_historical") }}),
    max_date as (

        select *
        from source
        where snapshot_date = (select max(snapshot_date) from source)

    )

select *
from max_date
