with
    base as (select * from {{ ref("prep_product_detail_central_sandbox") }}),
    final as (

        select
            dim_product_detail_id as dim_product_detail_id,
            product_id as product_id,
            dim_product_tier_id as dim_product_tier_id,
            product_rate_plan_id as product_rate_plan_id,
            product_rate_plan_charge_id as product_rate_plan_charge_id,
            product_rate_plan_name as product_rate_plan_name,
            product_rate_plan_charge_name as product_rate_plan_charge_name,
            product_name as product_name,
            product_sku as product_sku,
            product_tier_historical as product_tier_historical,
            product_tier_historical_short as product_tier_historical_short,
            product_tier_name as product_tier_name,
            product_tier_name_short as product_tier_name_short,
            product_delivery_type as product_delivery_type,
            service_type as service_type,
            is_reporter_license as is_reporter_license,
            effective_start_date as effective_start_date,
            effective_end_date as effective_end_date,
            product_ranking as product_ranking,
            is_oss_or_edu_rate_plan as is_oss_or_edu_rate_plan,
            billing_list_price as billing_list_price,
            annual_billing_list_price as annual_billing_list_price
        from base
    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@michellecooper",
            updated_by="@michellecooper",
            created_date="2022-03-31",
            updated_date="2022-03-31",
        )
    }}
