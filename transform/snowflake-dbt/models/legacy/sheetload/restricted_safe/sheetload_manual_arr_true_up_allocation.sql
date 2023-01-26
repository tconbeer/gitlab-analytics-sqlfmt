{{ config(tags=["mnpi"]) }}

with
    source as (

        select * from {{ ref("sheetload_manual_arr_true_up_allocation_source") }}

    )

select *
from source
