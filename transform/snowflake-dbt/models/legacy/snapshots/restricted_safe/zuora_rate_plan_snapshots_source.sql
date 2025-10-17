with
    source as (select * from {{ source("snapshots", "zuora_rateplan_snapshots") }}),
    renamed as (

        select
            id as rate_plan_id,
            name as rate_plan_name,
            -- keys
            subscriptionid as subscription_id,
            productid as product_id,
            productrateplanid as product_rate_plan_id,
            -- info
            amendmentid as amendement_id,
            amendmenttype as amendement_type,

            -- metadata
            updatedbyid as updated_by_id,
            updateddate as updated_date,
            createdbyid as created_by_id,
            createddate as created_date,
            deleted as is_deleted,

            -- snapshot metadata
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to

        from source

    )

select *
from renamed
