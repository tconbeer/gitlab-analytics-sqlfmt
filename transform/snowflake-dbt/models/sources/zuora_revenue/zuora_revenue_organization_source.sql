with
    zuora_revenue_organization as (

        select *
        from {{ source("zuora_revenue", "zuora_revenue_organization") }}
        qualify rank() OVER (partition by id order by incr_updt_dt desc) = 1

    ),
    renamed as (

        select

            id::varchar as zuora_revenue_organization_id,
            org_id::varchar as organization_id,
            org_name::varchar as organization_name,
            crtd_by::varchar as organization_created_by,
            crtd_dt::datetime as organization_created_date,
            updt_by::varchar as organization_updated_by,
            updt_dt::datetime as organization_updated_date,
            client_id::varchar as client_id,
            concat(crtd_prd_id::varchar, '01') as organization_created_period_id,
            incr_updt_dt::datetime as incremental_update_date,
            org_code::varchar as organization_code,
            entity_id::varchar as entity_id

        from zuora_revenue_organization

    )

select *
from renamed
