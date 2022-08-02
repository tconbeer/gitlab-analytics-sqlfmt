with
    source as (select * from {{ ref("sheetload_communication_certificate_source") }}),
    {{
        cleanup_certificates(
            "'communication_certificate'",
        )
    }}
