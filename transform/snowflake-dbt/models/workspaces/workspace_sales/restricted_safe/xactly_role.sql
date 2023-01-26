with source as (select * from {{ ref("xactly_role_source") }}) select * from source
