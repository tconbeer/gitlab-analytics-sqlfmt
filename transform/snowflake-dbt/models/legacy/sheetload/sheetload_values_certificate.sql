with
    source as (select * from {{ ref("sheetload_values_certificate_source") }}),
    {{ cleanup_certificates("'values_certificate'") }}
