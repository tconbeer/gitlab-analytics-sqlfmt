with base as (select * from {{ ref("sfdc_users_source") }}) select * from base
