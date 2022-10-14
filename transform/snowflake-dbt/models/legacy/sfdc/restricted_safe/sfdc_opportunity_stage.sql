with base as (select * from {{ ref("sfdc_opportunity_stage_source") }})

select *
from base
