with source as (select * from {{ ref("customers_db_license_seat_links_source") }})

select *
from source
