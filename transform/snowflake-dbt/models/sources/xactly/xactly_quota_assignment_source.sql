with
    source as (select * from {{ source("xactly", "xc_quota_assignment") }}),
    renamed as (

        select

            amount::float as amount,
            amount_unit_type_id::float as amount_unit_type_id,
            assignment_id::float as assignment_id,
            assignment_name::varchar as assignment_name,
            assignment_type::float as assignment_type,
            created_by_id::float as created_by_id,
            created_by_name::varchar as created_by_name,
            created_date::varchar as created_date,
            description::varchar as description,
            effective_end_period_id::float as effective_end_period_id,
            effective_start_period_id::float as effective_start_period_id,
            is_active::varchar as is_active,
            modified_by_id::float as modified_by_id,
            modified_by_name::varchar as modified_by_name,
            modified_date::varchar as modified_date,
            period_id::float as period_id,
            qta_asgnmt_id::varchar as qta_asgnmt_id,
            quota_assignment_id::float as quota_assignment_id,
            quota_id::float as quota_id

        from source

    )

select *
from renamed
