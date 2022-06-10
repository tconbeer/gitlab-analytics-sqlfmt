with
    source as (

        select * from {{ source("customers", "customers_db_license_seat_links") }}

    ),
    renamed as (

        select
            zuora_subscription_id::varchar as zuora_subscription_id,
            zuora_subscription_name::varchar as zuora_subscription_name,
            order_id::number as order_id,
            report_timestamp::timestamp as report_timestamp,
            report_timestamp::date as report_date,
            license_starts_on::date as license_starts_on,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            active_user_count::number as active_user_count,
            license_user_count::number as license_user_count,
            max_historical_user_count::number as max_historical_user_count
        from source
        qualify
            row_number() over (
                partition by zuora_subscription_id, report_date order by updated_at desc
            ) = 1

    )

select *
from renamed
