{{
    config(
        {
            "alias": "license_db_licenses_snapshots",
        }
    )
}}

with
    source as (

        select * from {{ source("snapshots", "license_db_licenses_snapshots") }}

    ),
    renamed as (

        select distinct
            dbt_scd_id::varchar as license_snapshot_id,
            id::number as license_id,
            company::varchar as company,
            users_count::number as users_count,
            license_md5::varchar as license_md5,
            expires_at::timestamp as license_expires_at,
            recurly_subscription_id::varchar as recurly_subscription_id,
            plan_name::varchar as plan_name,
            starts_at::timestamp as starts_at,
            zuora_subscription_id::varchar as zuora_subscription_id,
            previous_users_count::number as previous_users_count,
            trueup_quantity::number as trueup_quantity,
            trueup_from::timestamp as trueup_from,
            trueup_to::timestamp as trueup_to,
            plan_code::varchar as plan_code,
            trial::boolean as is_trial,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            "DBT_VALID_FROM"::timestamp as valid_from,
            "DBT_VALID_TO"::timestamp as valid_to
        from source

    )

select *
from renamed
