with source as (select * from {{ ref("zengrc_objective_source") }}) select * from source
