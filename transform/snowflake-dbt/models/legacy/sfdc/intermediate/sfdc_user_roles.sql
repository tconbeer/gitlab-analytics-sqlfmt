with base as (select * from {{ ref("sfdc_user_roles_source") }}) select * from base
