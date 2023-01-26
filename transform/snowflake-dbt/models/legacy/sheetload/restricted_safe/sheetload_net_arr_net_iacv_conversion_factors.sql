{{ config(tags=["mnpi"]) }}

with
    source as (

        select * from {{ ref("sheetload_net_arr_net_iacv_conversion_factors_source") }}

    )

select *
from source
