with
    source as (select * from {{ source("sheetload", "mapping_sdr_sfdc_bamboohr") }}),
    renamed as (

        select
            user_id::varchar as user_id,
            first_name::varchar as first_name,
            last_name::varchar as last_name,
            username::varchar as username,
            active::number as active,
            profile::varchar as profile,
            eeid::number as eeid,
            sdr_segment::varchar as sdr_segment,
            sdr_region::varchar as sdr_region,
            sdr_order_type::varchar as sdr_order_type

        from source

    )

select *
from renamed
