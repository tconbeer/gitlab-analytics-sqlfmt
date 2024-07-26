with
    source as (

        select * from {{ ref("demandbase_campaign_account_performance_source") }}

    )

select *
from source
