with
    source as (

        select *
        from {{ source("customers", "customers_db_licenses") }}
        qualify row_number() OVER (partition by id order by updated_at desc) = 1

    ),
    renamed as (

        select distinct
            id::number as license_id,
            name::varchar as name,
            company::varchar as company,
            email::varchar as email,
            users_count::number as license_user_count,
            license_file::varchar as license_file,
            expires_at::date as license_expire_date,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            plan_name::varchar as plan_name,
            starts_at::date as license_start_date,
            zuora_subscription_id::varchar as zuora_subscription_id,
            notes::varchar as license_notes,
            previous_users_count::number as previous_users_count,
            trueup_quantity::number as trueup_quantity,
            trueup_from::date as trueup_from_date,
            trueup_to::date as trueup_to_date,
            plan_code::varchar as plan_code,
            trial::boolean as is_trial,
            zuora_subscription_name::varchar as zuora_subscription_name,
            replace(license_file_md5::varchar, '-') as license_md5,
            creator_id::number as creator_id
        from source

    )

select *
from renamed
