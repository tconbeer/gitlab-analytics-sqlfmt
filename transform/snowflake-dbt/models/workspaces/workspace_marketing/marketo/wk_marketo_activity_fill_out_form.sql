with source as (select * from {{ ref("marketo_activity_fill_out_form_source") }})

select *
from source
