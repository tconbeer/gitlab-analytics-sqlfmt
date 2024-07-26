with
    source as (select * from {{ ref("sheetload_ic_collaboration_competency_source") }}),

    {{ cleanup_certificates("'ic_collaboration_competency'") }}
