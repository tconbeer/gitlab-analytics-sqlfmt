with source as (select * from {{ ref("netsuite_accounting_periods_source") }})

select *
from source
