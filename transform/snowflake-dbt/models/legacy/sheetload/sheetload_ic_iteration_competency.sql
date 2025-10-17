with
    source as (select * from {{ ref("sheetload_ic_iteration_competency_source") }}),

    {{ cleanup_certificates("'ic_iteration_competency'") }}
