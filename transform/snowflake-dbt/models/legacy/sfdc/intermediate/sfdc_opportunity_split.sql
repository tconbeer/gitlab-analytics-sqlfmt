with base as (select * from {{ ref("sfdc_opportunity_split_source") }})

select *
from base
