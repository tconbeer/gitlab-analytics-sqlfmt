{{ config(alias="mart_sales_funnel_target") }}


with
    date_details as (select * from {{ ref("wk_sales_date_details") }}),
    mart_sales_funnel_target as (

        select
            funnel_target.*,
            -- 20220214 NF: Temporary keys, until the SFDC key is exposed,
            case
                when funnel_target.order_type_name = '3. Growth'
                then '2. Growth'
                when funnel_target.order_type_name = '1. New - First Order'
                then '1. New'
                else '3. Other'
            end as deal_group,

            coalesce(
                funnel_target.sales_qualified_source_name, 'NA'
            ) as sales_qualified_source,
            lower(
                concat(
                    funnel_target.crm_user_sales_segment,
                    '-',
                    funnel_target.crm_user_geo,
                    '-',
                    funnel_target.crm_user_region,
                    '-',
                    funnel_target.crm_user_area,
                    '-',
                    sales_qualified_source,
                    '-',
                    funnel_target.order_type_name
                )
            ) as report_user_segment_geo_region_area_sqs_ot
        from {{ ref("mart_sales_funnel_target") }} funnel_target

    ),
    agg_demo_keys as (
        -- keys used for aggregated historical analysis
        select * from {{ ref("wk_sales_report_agg_demo_sqs_ot_keys") }}

    ),
    final as (

        select
            funnel_target.*,
            target_month.fiscal_quarter_name_fy as target_fiscal_quarter_name,
            target_month.first_day_of_fiscal_quarter as target_fiscal_quarter_date,

            agg_demo_keys.sales_team_cro_level,
            agg_demo_keys.sales_team_vp_level,
            agg_demo_keys.sales_team_avp_rd_level,
            agg_demo_keys.sales_team_asm_level,
            agg_demo_keys.sales_team_rd_asm_level,

            agg_demo_keys.key_segment,
            agg_demo_keys.key_sqs,
            agg_demo_keys.key_ot,

            agg_demo_keys.key_segment_geo,
            agg_demo_keys.key_segment_geo_sqs,
            agg_demo_keys.key_segment_geo_ot,

            agg_demo_keys.key_segment_geo_region,
            agg_demo_keys.key_segment_geo_region_sqs,
            agg_demo_keys.key_segment_geo_region_ot,

            agg_demo_keys.key_segment_geo_region_area,
            agg_demo_keys.key_segment_geo_region_area_sqs,
            agg_demo_keys.key_segment_geo_region_area_ot,

            agg_demo_keys.report_user_segment_geo_region_area

        from mart_sales_funnel_target funnel_target
        inner join
            date_details target_month
            on target_month.date_actual = funnel_target.target_month
        left join
            agg_demo_keys
            on funnel_target.report_user_segment_geo_region_area_sqs_ot
            = agg_demo_keys.report_user_segment_geo_region_area_sqs_ot
        where lower(funnel_target.deal_group) like any ('%growth%', '%new%')
    )

select *
from final
