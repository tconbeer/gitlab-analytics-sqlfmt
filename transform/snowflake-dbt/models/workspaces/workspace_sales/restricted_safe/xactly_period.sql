with source as (select * from {{ ref("xactly_period_source") }}) select * from source
