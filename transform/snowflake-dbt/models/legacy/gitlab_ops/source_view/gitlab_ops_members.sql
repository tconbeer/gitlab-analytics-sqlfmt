{{ config({"schema": "legacy"}) }}

with source as (select * from {{ ref("gitlab_ops_members_source") }})

select *
from source
