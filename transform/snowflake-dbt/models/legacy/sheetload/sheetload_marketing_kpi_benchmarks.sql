{{ config({"schema": "legacy"}) }}

with source as (select * from {{ ref("sheetload_marketing_kpi_benchmarks_source") }})

select *
from source
