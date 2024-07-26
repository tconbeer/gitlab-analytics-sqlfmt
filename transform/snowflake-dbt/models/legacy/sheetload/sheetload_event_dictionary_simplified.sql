with source as (select * from {{ ref("sheetload_event_dictionary_simplified_source") }})

select *
from source
