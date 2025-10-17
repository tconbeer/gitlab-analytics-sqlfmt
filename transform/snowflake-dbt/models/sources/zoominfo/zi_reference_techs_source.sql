with
    source as (select * from {{ source("zoominfo", "techs") }}),
    renamed as (

        select
            zi_c_tech_id::number as zi_c_tech_id,
            zi_c_tech_name::varchar as zi_c_tech_name,
            zi_c_category::varchar as zi_c_category,
            zi_c_category_parent::varchar as zi_c_category_parent,
            zi_c_vendor::varchar as zi_c_vendor,
            zi_c_tech_domain::varchar as zi_c_tech_domain,
            zi_c_description::varchar as zi_c_description
        from source

    )

select *
from renamed
