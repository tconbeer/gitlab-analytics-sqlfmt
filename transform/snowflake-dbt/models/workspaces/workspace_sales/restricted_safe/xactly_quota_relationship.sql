with source as (select * from {{ ref("xactly_quota_relationship_source") }})

select *
from source
