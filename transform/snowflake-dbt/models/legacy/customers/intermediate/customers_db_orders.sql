with source as (select * from {{ ref("customers_db_orders_source") }})

select *
from source
