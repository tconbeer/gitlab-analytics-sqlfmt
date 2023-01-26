with base as (select * from {{ ref("sfdc_opportunity_field_history_source") }})

select *
from base
