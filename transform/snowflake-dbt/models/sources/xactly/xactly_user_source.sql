with
    source as (select * from {{ source("xactly", "xc_user") }}),

    renamed as (

        select

            user_id::float as user_id,
            version::float as version,
            email::varchar as email,
            name::varchar as name,
            passwd_chg_dt::varchar as passwd_chg_dt,
            passwd_exp_dt::varchar as passwd_exp_dt,
            ip_addresses::varchar as ip_addresses,
            is_active::varchar as is_active,
            is_passwd_reset::varchar as is_passwd_reset,
            business_group_id::float as business_group_id,
            created_date::varchar as created_date,
            created_by_id::float as created_by_id,
            created_by_name::varchar as created_by_name,
            modified_date::varchar as modified_date,
            modified_by_id::float as modified_by_id,
            modified_by_name::varchar as modified_by_name,
            locale::varchar as locale,
            enabled::varchar as enabled,
            accepted_contract::varchar as accepted_contract,
            source_id::float as source_id,
            primary_role_type::varchar as primary_role_type,
            activated_date::varchar as activated_date,
            is_internal_user::varchar as is_internal_user,
            user_uuid::varchar as user_uuid


        from source

    )

select *
from renamed
