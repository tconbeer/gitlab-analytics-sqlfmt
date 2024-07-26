{{ config({"schema": "legacy"}) }}

with source as (select * from {{ ref("gitlab_dotcom_audit_events_source") }})

select *
from source
