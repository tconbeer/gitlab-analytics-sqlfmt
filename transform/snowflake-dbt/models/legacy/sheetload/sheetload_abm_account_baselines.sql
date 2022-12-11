{{ config({"schema": "legacy"}) }}

with source as (select * from {{ ref("sheetload_abm_account_baselines_source") }})

select *
from source
