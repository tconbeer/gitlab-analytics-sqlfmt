with base as (select * from {{ ref("sfdc_opportunity_contact_role_source") }})

select *
from base
