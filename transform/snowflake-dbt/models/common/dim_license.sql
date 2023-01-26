with
    tiers as (

        select *
        from {{ ref("prep_product_tier") }}
        where product_delivery_type = 'Self-Managed'

    ),
    license as (select * from {{ ref("prep_license") }}),
    environment as (select * from {{ ref("prep_environment") }}),
    final as (

        select
            -- Primary key
            license.dim_license_id,

            -- Foreign keys
            license.dim_subscription_id,
            license.dim_subscription_id_original,
            license.dim_subscription_id_previous,
            environment.dim_environment_id,
            tiers.dim_product_tier_id,

            -- Descriptive information
            license.license_md5,
            license.subscription_name,
            license.environment,
            license.license_user_count,
            license.license_plan,
            license.is_trial,
            license.is_internal,
            license.company,
            license.license_start_date,
            license.license_expire_date,
            license.created_at,
            license.updated_at
        from license
        left join
            tiers on lower(tiers.product_tier_historical_short) = license.license_plan
        left join environment on environment.environment = license.environment
    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@snalamaru",
            updated_by="@jpeguero",
            created_date="2021-01-08",
            updated_date="2021-09-22",
        )
    }}
