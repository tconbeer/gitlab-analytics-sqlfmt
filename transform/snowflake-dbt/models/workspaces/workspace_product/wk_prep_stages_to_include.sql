{{ config(tags=["mnpi_exception"]) }}

with
    smau_only as (

        select distinct stage_name
        from {{ ref("fct_monthly_usage_data") }}
        where is_smau = true

    )

select *
from smau_only
