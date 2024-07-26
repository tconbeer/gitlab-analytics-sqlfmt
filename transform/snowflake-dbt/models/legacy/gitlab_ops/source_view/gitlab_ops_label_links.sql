{{ config({"schema": "legacy"}) }}

with source as (select * from {{ ref("gitlab_ops_label_links_source") }})

select *
from source
