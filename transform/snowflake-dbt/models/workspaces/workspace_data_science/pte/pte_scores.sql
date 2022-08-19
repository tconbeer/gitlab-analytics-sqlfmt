with source as (select * from {{ ref("pte_scores_source") }}) select * from source
