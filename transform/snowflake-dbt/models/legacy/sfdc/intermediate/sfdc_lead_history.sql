with base as (select * from {{ ref("sfdc_lead_history_source") }}) select * from base
