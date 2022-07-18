{{ config({"schema": "legacy"}) }}

with
    licenses as (select * from {{ ref("license_db_licenses_source") }}),
    renamed as (

        select
            license_id,
            license_md5,
            zuora_subscription_id as subscription_id,
            zuora_subscription_name as subscription_name,
            users_count as license_user_count,
            plan_code as license_plan,
            is_trial,
            iff(
                lower(email) like '%@gitlab.com' and lower(company) like '%gitlab%',
                true,
                false
            ) as is_internal,
            company as company,
            starts_at::date as license_start_date,
            license_expires_at::date as license_expire_date,
            created_at,
            updated_at
        from licenses

    )


    {{
        dbt_audit(
            cte_ref="renamed",
            created_by="@derekatwood",
            updated_by="@msendal",
            created_date="2020-08-10",
            updated_date="2020-09-17",
        )
    }}
