{{ config({"schema": "legacy"}) }}

with source as (select * from {{ ref("gitlab_dotcom_shards_source") }})

select *
from source
