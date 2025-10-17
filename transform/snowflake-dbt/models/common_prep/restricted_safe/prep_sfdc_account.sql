{{
    config(
        {
            "materialized": "view",
        }
    )
}}

with
    sfdc_account as (

        select * from {{ ref("sfdc_account_source") }} where not is_deleted

    ),
    ultimate_parent_account as (

        select
            account_id,
            account_name,
            billing_country,
            df_industry,
            account_owner_team,
            tsp_territory,
            tsp_region,
            tsp_sub_region,
            tsp_area
        from sfdc_account
        where account_id = ultimate_parent_account_id

    ),
    sfdc_account_with_ultimate_parent as (

        select
            sfdc_account.account_id as dim_crm_account_id,
            ultimate_parent_account.account_id as ultimate_parent_account_id,
            {{ sales_segment_cleaning("sfdc_account.ultimate_parent_sales_segment") }}
            as ultimate_parent_sales_segment,
            ultimate_parent_account.billing_country as ultimate_parent_billing_country,
            ultimate_parent_account.df_industry as ultimate_parent_df_industry,
            ultimate_parent_account.tsp_territory as ultimate_parent_tsp_territory,
            {{ sales_segment_cleaning("sfdc_account.ultimate_parent_sales_segment") }}
            as sales_segment,
            case
                when
                    {{
                        sales_segment_cleaning(
                            "sfdc_account.ultimate_parent_sales_segment"
                        )
                    }} in ('Large', 'PubSec')
                then 'Large'
                else
                    {{
                        sales_segment_cleaning(
                            "sfdc_account.ultimate_parent_sales_segment"
                        )
                    }}
            end as sales_segment_grouped,
            sfdc_account.billing_country,
            sfdc_account.df_industry,
            sfdc_account.tsp_territory
        from sfdc_account
        left join
            ultimate_parent_account
            on sfdc_account.ultimate_parent_account_id
            = ultimate_parent_account.account_id

    ),
    sfdc_account_final as (

        select
            dim_crm_account_id as dim_crm_account_id,
            ultimate_parent_account_id as dim_parent_crm_account_id,
            trim(tsp_territory) as account_tsp_territory_clean,
            trim(ultimate_parent_tsp_territory) as parent_tsp_territory_clean,
            trim(split_part(df_industry, '-', 1)) as account_df_industry_clean,
            trim(
                split_part(ultimate_parent_df_industry, '-', 1)
            ) as parent_df_industry_clean,
            sales_segment as account_sales_segment_clean,
            sales_segment_grouped as account_sales_segment_grouped_clean,
            ultimate_parent_sales_segment as parent_sales_segment_clean,
            trim(split_part(billing_country, '-', 1)) as account_billing_country_clean,
            trim(
                split_part(ultimate_parent_billing_country, '-', 1)
            ) as parent_billing_country_clean,
            max(account_tsp_territory_clean) over (
                partition by upper(trim(account_tsp_territory_clean))
            ) as dim_account_sales_territory_name_source,
            max(parent_tsp_territory_clean) over (
                partition by upper(trim(parent_tsp_territory_clean))
            ) as dim_parent_sales_territory_name_source,
            max(account_df_industry_clean) over (
                partition by upper(trim(account_df_industry_clean))
            ) as dim_account_industry_name_source,
            max(parent_df_industry_clean) over (
                partition by upper(trim(parent_df_industry_clean))
            ) as dim_parent_industry_name_source,
            max(account_sales_segment_clean) over (
                partition by upper(trim(account_sales_segment_clean))
            ) as dim_account_sales_segment_name_source,
            max(account_sales_segment_grouped_clean) over (
                partition by upper(trim(account_sales_segment_grouped_clean))
            ) as dim_account_sales_segment_grouped_source,
            max(parent_sales_segment_clean) over (
                partition by upper(trim(parent_sales_segment_clean))
            ) as dim_parent_sales_segment_name_source,
            max(account_billing_country_clean) over (
                partition by upper(trim(account_billing_country_clean))
            ) as dim_account_location_country_name_source,
            max(parent_billing_country_clean) over (
                partition by upper(trim(parent_billing_country_clean))
            ) as dim_parent_location_country_name_source

        from sfdc_account_with_ultimate_parent

    )

    {{
        dbt_audit(
            cte_ref="sfdc_account_final",
            created_by="@paul_armstrong",
            updated_by="@jpeguero",
            created_date="2020-10-30",
            updated_date="2021-04-26",
        )
    }}
