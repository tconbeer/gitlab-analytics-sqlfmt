with
    product_tier_mapping as (select * from {{ ref("map_product_tier_api_sandbox") }}),
    mapping as (

        select distinct
            product_tier_historical,
            product_tier,
            product_delivery_type,
            product_ranking
        from product_tier_mapping

        union all

        select
            'SaaS - Free' as product_tier_historical,
            'SaaS - Free' as product_tier,
            'SaaS' as product_delivery_type,
            0 as product_ranking

        union all

        select
            'Self-Managed - Core' as product_tier_historical,
            'Self-Managed - Free' as product_tier,
            'Self-Managed' as product_delivery_type,
            0 as product_ranking

        union all

        select
            'SaaS - Trial: Gold' as product_tier_historical,
            'SaaS - Trial: Ultimate' as product_tier,
            'SaaS' as product_delivery_type,
            0 as product_ranking

        union all

        select
            'Self-Managed - Trial: Ultimate' as product_tier_historical,
            'Self-Managed - Trial: Ultimate' as product_tier,
            'Self-Managed' as product_delivery_type,
            0 as product_ranking

    ),
    final as (

        select
            {{ dbt_utils.surrogate_key(["product_tier_historical"]) }}
            as dim_product_tier_id,
            product_tier_historical,
            split_part(
                product_tier_historical, ' - ', -1
            ) as product_tier_historical_short,
            product_tier as product_tier_name,
            split_part(product_tier, ' - ', -1) as product_tier_name_short,
            product_delivery_type,
            product_ranking
        from mapping

        union all

        select
            md5('-1') as dim_product_tier_id,
            '(Unknown Historical Tier)' as product_tier_historical,
            '(Unknown Historical Tier Name)' as product_tier_historical_short,
            '(Unknown Tier)' as product_tier_name,
            '(Unknown Tier Name)' as product_tier_name_short,
            '(Unknown Delivery Type)' as product_delivery_type,
            -1 as product_ranking

    )


    {{
        dbt_audit(
            cte_ref="final",
            created_by="@ken_aguilar",
            updated_by="@ken_aguilar",
            created_date="2021-08-26",
            updated_date="2021-08-26",
        )
    }}
