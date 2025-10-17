with source as (select * from {{ ref("netsuite_consolidated_exchange_rates_source") }})

select *
from source
where is_fivetran_deleted = false
