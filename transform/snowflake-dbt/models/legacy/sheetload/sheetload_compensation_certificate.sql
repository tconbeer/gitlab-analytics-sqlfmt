with
    source as (select * from {{ ref("sheetload_compensation_certificate_source") }}),
    {{ cleanup_certificates("'compensation_certificate'") }}
