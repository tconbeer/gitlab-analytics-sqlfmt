with
    zuora_product as (select * from {{ ref("zuora_product_source") }}),
    zuora_product_rate_plan as (

        select * from {{ ref("zuora_product_rate_plan_source") }}

    ),
    zuora_product_rate_plan_charge as (

        select * from {{ ref("zuora_product_rate_plan_charge_source") }}

    ),
    zuora_product_rate_plan_charge_tier as (

        select * from {{ ref("zuora_product_rate_plan_charge_tier_source") }}

    ),
    common_product_tier as (select * from {{ ref("prep_product_tier") }}),
    common_product_tier_mapping as (select * from {{ ref("map_product_tier") }}),
    joined as (

        select
            -- ids
            zuora_product_rate_plan_charge.product_rate_plan_charge_id
            as dim_product_detail_id,
            zuora_product.product_id as product_id,
            common_product_tier.dim_product_tier_id as dim_product_tier_id,
            zuora_product_rate_plan.product_rate_plan_id as product_rate_plan_id,
            zuora_product_rate_plan_charge.product_rate_plan_charge_id
            as product_rate_plan_charge_id,

            -- fields
            zuora_product_rate_plan.product_rate_plan_name as product_rate_plan_name,
            zuora_product_rate_plan_charge.product_rate_plan_charge_name
            as product_rate_plan_charge_name,
            zuora_product.product_name as product_name,
            zuora_product.sku as product_sku,
            common_product_tier.product_tier_historical as product_tier_historical,
            common_product_tier.product_tier_historical_short
            as product_tier_historical_short,
            common_product_tier.product_tier_name as product_tier_name,
            common_product_tier.product_tier_name_short as product_tier_name_short,
            common_product_tier_mapping.product_delivery_type as product_delivery_type,
            case
                when
                    lower(
                        zuora_product_rate_plan.product_rate_plan_name
                    ) like '%support%'
                then 'Support Only'
                else 'Full Service'
            end as service_type,
            lower(
                zuora_product_rate_plan.product_rate_plan_name
            ) like '%reporter access%' as is_reporter_license,
            zuora_product.effective_start_date as effective_start_date,
            zuora_product.effective_end_date as effective_end_date,
            common_product_tier_mapping.product_ranking as product_ranking,
            case
                when
                    lower(zuora_product_rate_plan.product_rate_plan_name) like any (
                        '%oss%', '%edu%'
                    )
                then true
                else false
            end as is_oss_or_edu_rate_plan,
            min(zuora_product_rate_plan_charge_tier.price) as billing_list_price
        from zuora_product
        inner join
            zuora_product_rate_plan
            on zuora_product.product_id = zuora_product_rate_plan.product_id
        inner join
            zuora_product_rate_plan_charge
            on zuora_product_rate_plan.product_rate_plan_id
            = zuora_product_rate_plan_charge.product_rate_plan_id
        inner join
            zuora_product_rate_plan_charge_tier
            on zuora_product_rate_plan_charge.product_rate_plan_charge_id
            = zuora_product_rate_plan_charge_tier.product_rate_plan_charge_id
        left join
            common_product_tier_mapping
            on zuora_product_rate_plan_charge.product_rate_plan_id
            = common_product_tier_mapping.product_rate_plan_id
        left join
            common_product_tier
            on common_product_tier_mapping.product_tier_historical
            = common_product_tier.product_tier_historical
        where
            zuora_product.is_deleted = false
            and zuora_product_rate_plan_charge_tier.currency = 'USD'
            {{ dbt_utils.group_by(n=20) }}
        order by 1, 3

    ),  -- add annualized billing list price
    final as (

        select
            joined.*,
            case
                when
                    lower(product_rate_plan_name) like '%month%' or lower(
                        product_rate_plan_charge_name
                    ) like '%month%' or lower(product_name) like '%month%'
                then (billing_list_price * 12)
                when
                    lower(product_rate_plan_name) like '%2 year%' or lower(
                        product_rate_plan_charge_name
                    ) like '%2 year%' or lower(product_name) like '%2 year%'
                then (billing_list_price / 2)
                when
                    lower(product_rate_plan_name) like '%3 year%' or lower(
                        product_rate_plan_charge_name
                    ) like '%3 year%' or lower(product_name) like '%3 year%'
                then (billing_list_price / 3)
                when
                    lower(product_rate_plan_name) like '%4 year%' or lower(
                        product_rate_plan_charge_name
                    ) like '%4 year%' or lower(product_name) like '%4 year%'
                then (billing_list_price / 4)
                when
                    lower(product_rate_plan_name) like '%5 year%' or lower(
                        product_rate_plan_charge_name
                    ) like '%5 year%' or lower(product_name) like '%5 year%'
                then (billing_list_price / 5)
                else billing_list_price
            end as annual_billing_list_price
        from joined

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@ischweickartDD",
            updated_by="@mcooperDD",
            created_date="2020-12-16",
            updated_date="2021-01-26",
        )
    }}
