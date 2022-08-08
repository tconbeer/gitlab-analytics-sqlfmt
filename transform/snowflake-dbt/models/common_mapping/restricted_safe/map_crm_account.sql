with
    account_prep as (select * from {{ ref("prep_sfdc_account") }}),
    sales_segment as (select * from {{ ref("prep_sales_segment") }}),
    sales_territory as (select * from {{ ref("prep_sales_territory") }}),
    industry as (select * from {{ ref("prep_industry") }}),
    location_country as (select * from {{ ref("prep_location_country") }}),
    final as (

        select
            {{ get_keyed_nulls("account_prep.dim_parent_crm_account_id") }}
            as dim_parent_crm_account_id,
            {{ get_keyed_nulls("account_prep.dim_crm_account_id") }}
            as dim_crm_account_id,
            {{ get_keyed_nulls("sales_segment_ultimate_parent.dim_sales_segment_id") }}
            as dim_parent_sales_segment_id,
            {{
                get_keyed_nulls(
                    "sales_territory_ultimate_parent.dim_sales_territory_id"
                )
            }} as dim_parent_sales_territory_id,
            {{ get_keyed_nulls("industry_ultimate_parent.dim_industry_id") }}
            as dim_parent_industry_id,
            {{
                get_keyed_nulls(
                    "location_country_ultimate_parent.dim_location_country_id::varchar"
                )
            }} as dim_parent_location_country_id,
            {{
                get_keyed_nulls(
                    "location_country_ultimate_parent.dim_location_region_id"
                )
            }} as dim_parent_location_region_id,
            {{ get_keyed_nulls("sales_segment.dim_sales_segment_id") }}
            as dim_account_sales_segment_id,
            {{ get_keyed_nulls("sales_territory.dim_sales_territory_id") }}
            as dim_account_sales_territory_id,
            {{ get_keyed_nulls("industry.dim_industry_id") }}
            as dim_account_industry_id,
            {{ get_keyed_nulls("location_country.dim_location_country_id::varchar") }}
            as dim_account_location_country_id,
            {{ get_keyed_nulls("location_country.dim_location_region_id") }}
            as dim_account_location_region_id
        from account_prep
        left join
            sales_segment as sales_segment_ultimate_parent
            on account_prep.dim_parent_sales_segment_name_source
            = sales_segment_ultimate_parent.sales_segment_name
        left join
            sales_territory as sales_territory_ultimate_parent
            on account_prep.dim_parent_sales_territory_name_source
            = sales_territory_ultimate_parent.sales_territory_name
        left join
            industry as industry_ultimate_parent
            on account_prep.dim_parent_industry_name_source
            = industry_ultimate_parent.industry_name
        left join
            location_country as location_country_ultimate_parent
            on account_prep.dim_parent_location_country_name_source
            = location_country_ultimate_parent.country_name
        left join
            sales_segment
            on account_prep.dim_account_sales_segment_name_source
            = sales_segment.sales_segment_name
        left join
            sales_territory
            on account_prep.dim_account_sales_territory_name_source
            = sales_territory.sales_territory_name
        left join
            industry
            on account_prep.dim_account_industry_name_source = industry.industry_name
        left join
            location_country
            on account_prep.dim_account_location_country_name_source
            = location_country.country_name

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@snalamaru",
            updated_by="@pmcooperDD",
            created_date="2020-11-23",
            updated_date="2021-03-04",
        )
    }}
