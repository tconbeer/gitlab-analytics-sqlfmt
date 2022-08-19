with source as (select * from {{ ref("handbook_values_page_contributors_source") }})

select *
from source
