with base as (select * from {{ ref("sfdc_opportunity_team_member_source") }})

select *
from base
