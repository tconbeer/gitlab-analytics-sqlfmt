with
    customers_db_licenses as (select * from {{ ref("customers_db_licenses_source") }}),
    original_subscription as (select * from {{ ref("zuora_subscription_source") }}),
    licenses as (

        select
            license_id as dim_license_id,
            license_md5,
            zuora_subscription_id as dim_subscription_id,
            zuora_subscription_name as subscription_name,
            'Customers Portal' as environment,
            license_user_count,
            iff(plan_code is null or plan_code = '', 'core', plan_code) as license_plan,
            is_trial,
            iff(
                lower(email) like '%@gitlab.com' and lower(company) like '%gitlab%',
                true,
                false
            ) as is_internal,
            company,
            license_start_date,
            license_expire_date,
            created_at,
            updated_at
        from customers_db_licenses

    ),
    renamed as (

        select
            -- Primary Key
            licenses.dim_license_id,

            -- Foreign Keys
            licenses.dim_subscription_id,
            original_subscription.original_id as dim_subscription_id_original,
            original_subscription.previous_subscription_id
            as dim_subscription_id_previous,

            -- Descriptive information
            licenses.license_md5,
            licenses.subscription_name,
            licenses.environment,
            licenses.license_user_count,
            licenses.license_plan,
            licenses.is_trial,
            licenses.is_internal,
            licenses.company,
            licenses.license_start_date,
            licenses.license_expire_date,
            licenses.created_at,
            licenses.updated_at

        from licenses
        left join
            original_subscription
            on licenses.dim_subscription_id = original_subscription.subscription_id

    )

    {{
        dbt_audit(
            cte_ref="renamed",
            created_by="@snalamaru",
            updated_by="@chrissharp",
            created_date="2021-01-08",
            updated_date="2022-01-20",
        )
    }}
