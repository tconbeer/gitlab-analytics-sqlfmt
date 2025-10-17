with
    source as (select * from {{ ref("sheetload_ic_results_competency_source") }}),

    {{ cleanup_certificates("'ic_results_competency'") }}
