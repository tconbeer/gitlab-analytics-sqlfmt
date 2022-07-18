with
    source as (

        select * from {{ ref("sheetload_manual_downgrade_dotcom_tracking_source") }}

    )

select *
from source
