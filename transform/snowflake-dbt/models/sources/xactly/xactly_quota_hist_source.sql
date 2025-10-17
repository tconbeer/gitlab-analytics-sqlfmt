with
    source as (select * from {{ source("xactly", "xc_quota_hist") }}),
    renamed as (

        select

            classification_name::varchar as classification_name,
            classification_type::float as classification_type,
            created_by_id::float as created_by_id,
            created_by_name::varchar as created_by_name,
            created_date::varchar as created_date,
            description::varchar as description,
            is_active::varchar as is_active,
            modified_by_id::float as modified_by_id,
            modified_by_name::varchar as modified_by_name,
            modified_date::varchar as modified_date,
            name::varchar as name,
            object_id::float as object_id,
            quota_id::float as quota_id,
            quota_interval_id::float as quota_interval_id,
            quota_interval_name::varchar as quota_interval_name,
            quota_period_id::float as quota_period_id,
            quota_period_name::varchar as quota_period_name,
            quota_value::float as quota_value,
            quota_value_unit_type_id::float as quota_value_unit_type_id,
            quota_value_unit_type_name::varchar as quota_value_unit_type_name,
            source_id::float as source_id

        from source

    )

select *
from renamed
