with
    source_model as (

        select * from {{ ref("sheetload_usage_ping_metrics_sections_source") }}

    )

select *
from source_model
