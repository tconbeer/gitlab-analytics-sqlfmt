with
    source as (

        select *
        from {{ source("customers", "customers_db_orders") }}
        qualify row_number() over (partition by id order by updated_at desc) = 1

    ),
    renamed as (

        select distinct
            id::number as order_id,
            customer_id::number as customer_id,
            product_rate_plan_id::varchar as product_rate_plan_id,
            subscription_id::varchar as subscription_id,
            subscription_name::varchar as subscription_name,
            {{ zuora_slugify("subscription_name") }}::varchar
            as subscription_name_slugify,
            start_date::timestamp as order_start_date,
            end_date::timestamp as order_end_date,
            quantity::number as order_quantity,
            created_at::timestamp as order_created_at,
            updated_at::timestamp as order_updated_at,
            try_to_decimal(nullif(gl_namespace_id, ''))::varchar as gitlab_namespace_id,
            nullif(gl_namespace_name, '')::varchar as gitlab_namespace_name,
            amendment_type::varchar as amendment_type,
            trial::boolean as order_is_trial,
            last_extra_ci_minutes_sync_at::timestamp as last_extra_ci_minutes_sync_at,
            zuora_account_id::varchar as zuora_account_id,
            increased_billing_rate_notified_at::timestamp
            as increased_billing_rate_notified_at,
            source::varchar as order_source
        from source

    )

select *
from renamed
