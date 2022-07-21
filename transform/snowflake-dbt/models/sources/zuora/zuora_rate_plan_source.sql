with
    source as (select * from {{ source("zuora", "rate_plan") }}),
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
            deleted as is_deleted

        from source

    )

select *
from renamed
