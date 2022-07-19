{{ config({"schema": "legacy"}) }}

with
    source as (

        select *
        from {{ ref("sheetload_location_factor_temporary_2020_december_source") }}

    )

select *
from source
