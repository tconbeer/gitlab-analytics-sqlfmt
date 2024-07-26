with source as (select * from {{ ref("sfdc_record_type_source") }}) select * from source
