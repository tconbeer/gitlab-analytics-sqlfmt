with
    source as (

        select * from {{ ref("sfdc_campaign_member_source") }} where is_deleted = false

    )

select *
from source
