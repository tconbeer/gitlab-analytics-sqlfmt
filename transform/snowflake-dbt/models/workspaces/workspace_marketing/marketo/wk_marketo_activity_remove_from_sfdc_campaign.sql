with
    source as (

        select * from {{ ref("marketo_activity_remove_from_sfdc_campaign_source") }}

    )

select *
from source
