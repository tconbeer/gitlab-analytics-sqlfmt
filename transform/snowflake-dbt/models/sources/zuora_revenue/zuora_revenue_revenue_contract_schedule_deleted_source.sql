with
    zuora_revenue_revenue_contract_schedule_deleted as (

        select distinct *
        from
            {{
                source(
                    "zuora_revenue", "zuora_revenue_revenue_contract_schedule_deleted"
                )
            }}
        qualify rank() OVER (partition by schd_id order by incr_updt_dt desc) = 1

    ),
    renamed as (

        select

            schd_id::varchar as revenue_contract_schedule_id,
            client_id::varchar as client_id,
            deleted_time::datetime as revenue_contract_schedule_deleted_at,
            crtd_by::varchar as revenue_contract_schedule_created_by,
            crtd_dt::datetime as revenue_contract_schedule_created_date,
            updt_by::varchar as revenue_contract_schedule_updated_by,
            updt_dt::datetime as revenue_contract_schedule_updated_date,
            incr_updt_dt::datetime as incremental_update_date

        from zuora_revenue_revenue_contract_schedule_deleted

    )

select *
from renamed
