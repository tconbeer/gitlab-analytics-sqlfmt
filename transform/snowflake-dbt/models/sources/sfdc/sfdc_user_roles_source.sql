with base as (select * from {{ source("salesforce", "user_role") }}) select * from base
