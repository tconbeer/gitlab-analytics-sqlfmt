with
    source as (select * from {{ ref("sheetload_ally_certificate_source") }}),
    {{ cleanup_certificates("'ally_certificate'") }}
