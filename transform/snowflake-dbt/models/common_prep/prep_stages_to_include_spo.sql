{{ config(tags=["product"]) }}

with
    source as (

        select distinct stage_name
        from {{ ref("sheetload_usage_ping_metrics_sections") }}
        where is_smau

    )

select *
from source
