with
    source as (select * from {{ ref("sheetload_ic_dib_comptency_source") }}),

    {{ cleanup_certificates("'ic_dib_comptency'") }}
