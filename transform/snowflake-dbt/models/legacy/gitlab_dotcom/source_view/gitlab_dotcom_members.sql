{{ config({"schema": "legacy"}) }}

with source as (select * from {{ ref("gitlab_dotcom_members_source") }})

select *
from source
