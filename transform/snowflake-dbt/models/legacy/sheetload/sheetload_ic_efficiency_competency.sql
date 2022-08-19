with
    source as (select * from {{ ref("sheetload_ic_efficiency_competency_source") }}),

    {{ cleanup_certificates("'ic_efficiency_competency'") }}
