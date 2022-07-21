with source as (select * from {{ ref("ga360_session_source") }}) select * from source
