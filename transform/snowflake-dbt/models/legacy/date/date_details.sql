with source as (select * from {{ ref("date_details_source") }}) select * from source
