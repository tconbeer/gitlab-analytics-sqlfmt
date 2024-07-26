with source as (select * from {{ ref("gitlab_ops_users_source") }}) select * from source
