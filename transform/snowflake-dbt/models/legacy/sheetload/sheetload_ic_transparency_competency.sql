with
    source as (select * from {{ ref("sheetload_ic_transparency_competency_source") }}),

    {{ cleanup_certificates("'ic_transparency_competency'") }}
