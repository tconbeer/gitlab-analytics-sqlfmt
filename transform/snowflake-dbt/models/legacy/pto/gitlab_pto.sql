with source as (select * from {{ ref("gitlab_pto_source") }}) select * from source
