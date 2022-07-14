with
    source as (

        select *
        from {{ ref("marketo_activity_change_status_in_sfdc_campaign_source") }}

    )

select *
from source
