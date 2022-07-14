{{ config(tags=["mnpi"]) }}

with
    zuora_revenue_revenue_contract_line as (

        select *
        from {{ source("zuora_revenue", "zuora_revenue_revenue_contract_line") }}
        qualify rank() OVER (partition by id order by incr_updt_dt desc) = 1

    ),
    renamed as (

        select
        from zuora_revenue_revenue_contract_line

    )

select *
from renamed
