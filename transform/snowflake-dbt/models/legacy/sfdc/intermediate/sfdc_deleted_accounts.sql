{{ config(tags=["mnpi_exception"]) }}

with
    source as (

        select *
        from {{ ref("sfdc_account_source") }}
        where account_id is not null and is_deleted = true
    )

select
    a.account_id as sfdc_account_id,
    coalesce(b.master_record_id, a.master_record_id) as sfdc_master_record_id
from source a
left join source b on a.master_record_id = b.account_id
