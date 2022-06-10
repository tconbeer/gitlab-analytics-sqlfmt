with
    sfdc_account_snapshots as (

        select * from {{ ref("sfdc_account_snapshots_base_clean") }}

    )

select *
from sfdc_account_snapshots
