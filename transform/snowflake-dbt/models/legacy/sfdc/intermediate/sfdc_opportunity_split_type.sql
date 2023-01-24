with base as (select * from {{ ref("sfdc_opportunity_split_type_source") }})

select *
from base
