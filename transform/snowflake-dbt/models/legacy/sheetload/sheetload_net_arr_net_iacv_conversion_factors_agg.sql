with
    source as (

        select *
        from {{ ref("sheetload_net_arr_net_iacv_conversion_factors_agg_source") }}

    )

select *
from source
