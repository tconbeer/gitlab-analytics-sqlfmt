with source as (select * from {{ ref("xactly_quota_source") }}) select * from source
