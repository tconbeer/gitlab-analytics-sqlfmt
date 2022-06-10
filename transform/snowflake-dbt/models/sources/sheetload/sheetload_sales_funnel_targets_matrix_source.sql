{{ config(tags=["mnpi"]) }}

with
    source as (select * from {{ source("sheetload", "sales_funnel_targets_matrix") }}),
    renamed as (

        select
            kpi_name::varchar as kpi_name,
            month::varchar as month,
            opportunity_source::varchar as opportunity_source,
            order_type::varchar as order_type,
            area::varchar as area, replace (
                allocated_target, ',', ''
            )::float as allocated_target,
            user_segment::varchar as user_segment,
            user_geo::varchar as user_geo,
            user_region::varchar as user_region,
            user_area::varchar as user_area,
            to_timestamp(to_numeric("_UPDATED_AT"))::timestamp as last_updated_at
        from source

    )

select *
from renamed
