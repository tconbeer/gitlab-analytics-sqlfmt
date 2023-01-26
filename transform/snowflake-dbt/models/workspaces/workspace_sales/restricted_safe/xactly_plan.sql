with source as (select * from {{ ref("xactly_plan_source") }}) select * from source
