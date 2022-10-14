with
    source as (

        select *
        from {{ source("license", "licenses") }}
        qualify
            row_number() over (partition by id order by updated_at::timestamp desc) = 1

    ),
    renamed as (

        select
            id::number as license_id,
            company::varchar as company,
            users_count::number as users_count,
            email::varchar as email,
            license_md5::varchar as license_md5,
            case
                when expires_at is null
                then null::timestamp
                when split_part(expires_at, '-', 1)::number > 9999
                then '9999-12-30 00:00:00.000 +00'::timestamp
                else expires_at::timestamp
            end as license_expires_at,
            plan_name::varchar as plan_name,
            starts_at::timestamp as starts_at,
            nullif(zuora_subscription_name, '')::varchar as zuora_subscription_name,
            nullif(zuora_subscription_id, '')::varchar as zuora_subscription_id,
            previous_users_count::number as previous_users_count,
            trueup_quantity::number as trueup_quantity,
            trueup_from::timestamp as trueup_from,
            trueup_to::timestamp as trueup_to,
            plan_code::varchar as plan_code,
            trial::boolean as is_trial,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at
        from source

    )

select *
from renamed
