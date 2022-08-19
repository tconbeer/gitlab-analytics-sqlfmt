with
    source as (select * from {{ source("zuora_central_sandbox", "rate_plan") }}),
    renamed as (

        select
            id as rate_plan_id,
            name as rate_plan_name,
            -- keys
            subscription_id as subscription_id,
            product_id as product_id,
            product_rate_plan_id as product_rate_plan_id,
            -- info
            amendment_id as amendment_id,
            amendment_type as amendment_type,

            -- metadata
            updated_by_id as updated_by_id,
            updated_date as updated_date,
            created_by_id as created_by_id,
            created_date as created_date,
            _fivetran_deleted as is_deleted

        from source

    )

select *
from renamed
