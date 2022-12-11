with source as (select * from {{ ref("bizible_page_views_source_pii") }})

select *
from source
