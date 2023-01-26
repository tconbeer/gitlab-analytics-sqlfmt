with source as (select * from {{ ref("xactly_position_source") }}) select * from source
