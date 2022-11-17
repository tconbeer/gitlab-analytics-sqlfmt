with
    source as (

        select * from {{ source("zuora_central_sandbox", "product_rate_plan_charge") }}

    ),
    renamed as (

        select
            id as product_rate_plan_charge_id,
            product_rate_plan_id as product_rate_plan_id,
            name as product_rate_plan_charge_name
        from source

    )

select *
from renamed
