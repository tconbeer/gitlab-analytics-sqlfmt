with source as (select * from {{ ref("bizible_form_submits_source_pii") }})

select *
from source
