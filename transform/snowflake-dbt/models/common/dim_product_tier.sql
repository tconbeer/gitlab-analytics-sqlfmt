with
    prep_product_tier as (select * from {{ ref("prep_product_tier") }}),
    final as (

        select
            dim_product_tier_id,
            product_tier_historical,
            product_tier_historical_short,
            product_tier_name,
            product_tier_name_short,
            product_delivery_type,
            product_ranking
        from prep_product_tier

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@snalamaru",
            updated_by="@ischweickartDD",
            created_date="2020-12-28",
            updated_date="2021-01-26",
        )
    }}
