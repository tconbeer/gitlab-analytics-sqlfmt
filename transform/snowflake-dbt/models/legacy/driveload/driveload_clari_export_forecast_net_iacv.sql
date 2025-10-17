with
    source as (

        select * from {{ ref("driveload_clari_export_forecast_net_iacv_source") }}

    )

select *
from source
