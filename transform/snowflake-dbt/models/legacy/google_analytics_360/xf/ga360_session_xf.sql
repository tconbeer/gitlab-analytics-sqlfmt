with sessions as (select * from {{ ref("ga360_session") }}) select * from sessions
