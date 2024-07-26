with source as (select * from {{ ref("xactly_user_source") }}) select * from source
