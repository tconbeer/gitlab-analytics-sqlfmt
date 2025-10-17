with source as (select * from {{ ref("bizible_campaign_members_source_pii") }})

select *
from source
