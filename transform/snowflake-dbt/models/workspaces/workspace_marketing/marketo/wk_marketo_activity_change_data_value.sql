with source as (select * from {{ ref("marketo_activity_change_data_value_source") }})

select *
from source
