with
    source as (select * from {{ source("xactly", "xc_period") }}),

    renamed as (

        select

            period_id::float as period_id,
            version::float as version,
            name::varchar as name,
            start_date::varchar as start_date,
            end_date::varchar as end_date,
            is_active::varchar as is_active,
            is_open::varchar as is_open,
            parent_period_id::float as parent_period_id,
            period_type_id_fk::float as period_type_id_fk,
            calendar_id::float as calendar_id,
            order_number::float as order_number,
            created_date::varchar as created_date,
            created_by_id::float as created_by_id,
            created_by_name::varchar as created_by_name,
            modified_date::varchar as modified_date,
            modified_by_id::float as modified_by_id,
            modified_by_name::varchar as modified_by_name,
            is_published::varchar as is_published,
            is_visible::varchar as is_visible,
            is_etl_excluded::varchar as is_etl_excluded,
            is_hidden::varchar as is_hidden,
            is_calc_period::varchar as is_calc_period

        from source

    )

select *
from renamed
