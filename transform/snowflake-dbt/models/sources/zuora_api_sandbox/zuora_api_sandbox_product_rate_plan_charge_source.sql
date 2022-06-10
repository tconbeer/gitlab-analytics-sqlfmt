with
    source as (

        select * from {{ source("zuora_api_sandbox", "product_rate_plan_charge") }}

    ),
    renamed as (

        select
            id as product_rate_plan_charge_id,
            productrateplanid as product_rate_plan_id,
            name as product_rate_plan_charge_name
        from source

    )

select *
from renamed
