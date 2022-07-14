{{ config(tags=["mnpi"]) }}

with
    source as (

        select * from {{ source("sheetload", "net_arr_net_iacv_conversion_factors") }}

    ),
    renamed as (

        select
            opportunity_id::varchar as opportunity_id,
            order_type_stamped::varchar as order_type_stamped,
            user_segment::varchar as user_segment,
            net_iacv::number as net_iacv,
            net_arr::number as net_arr,
            ratio_net_iacv_to_net_arr::number as ratio_net_iacv_to_net_arr
        from source

    )

select *
from renamed
