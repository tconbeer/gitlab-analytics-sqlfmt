with
    source as (select * from {{ source("xactly", "xc_position") }}),
    renamed as (

        select

            business_group_id::float as business_group_id,
            created_by_id::float as created_by_id,
            created_by_name::varchar as created_by_name,
            created_date::varchar as created_date,
            credit_end_date::varchar as credit_end_date,
            credit_start_date::varchar as credit_start_date,
            descr::varchar as descr,
            effective_end_date::varchar as effective_end_date,
            effective_start_date::varchar as effective_start_date,
            incent_end_date::varchar as incent_end_date,
            incent_st_date::varchar as incent_st_date,
            is_active::varchar as is_active,
            is_master::varchar as is_master,
            master_position_id::float as master_position_id,
            modified_by_id::float as modified_by_id,
            modified_by_name::varchar as modified_by_name,
            modified_date::varchar as modified_date,
            name::varchar as name,
            parent_position_id::float as parent_position_id,
            parent_record_id::float as parent_record_id,
            participant_id::float as participant_id,
            pos_group_id::float as pos_group_id,
            position_id::float as position_id,
            title_id::float as title_id

        from source

    )

select *
from renamed
