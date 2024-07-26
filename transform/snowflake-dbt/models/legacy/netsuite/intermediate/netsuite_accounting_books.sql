with source as (select * from {{ ref("netsuite_accounting_books_source") }})

select *
from source
