{{ config(alias="report_agg_demo_sqs_ot_keys") }}

with
    sfdc_account_xf as (select * from {{ ref("sfdc_accounts_xf") }}),
    opportunity as (select * from {{ ref("sfdc_opportunity") }}),
    sfdc_opportunity_xf as (

        select

            lower(opportunity.user_segment_stamped) as report_opportunity_user_segment,
            lower(opportunity.user_geo_stamped) as report_opportunity_user_geo,
            lower(opportunity.user_region_stamped) as report_opportunity_user_region,
            lower(opportunity.user_area_stamped) as report_opportunity_user_area,
            lower(opportunity.order_type_stamped) as order_type_stamped,

            case
                when opportunity.sales_qualified_source = 'BDR Generated'
                then 'SDR Generated'
                else coalesce(opportunity.sales_qualified_source, 'other')
            end as sales_qualified_source,

            -- medium level grouping of the order type field
            case
                when opportunity.order_type_stamped = '1. New - First Order'
                then '1. New'
                when
                    opportunity.order_type_stamped in (
                        '2. New - Connected', '3. Growth'
                    )
                then '2. Growth'
                when opportunity.order_type_stamped in ('4. Contraction')
                then '3. Contraction'
                when
                    opportunity.order_type_stamped in (
                        '5. Churn - Partial', '6. Churn - Final'
                    )
                then '4. Churn'
                else '5. Other'
            end as deal_category,

            case
                when opportunity.order_type_stamped = '1. New - First Order'
                then '1. New'
                when
                    opportunity.order_type_stamped in (
                        '2. New - Connected',
                        '3. Growth',
                        '5. Churn - Partial',
                        '6. Churn - Final',
                        '4. Contraction'
                    )
                then '2. Growth'
                else '3. Other'
            end as deal_group,

            account.account_owner_user_segment,
            account.account_owner_user_geo,
            account.account_owner_user_region,
            account.account_owner_user_area

        from opportunity
        left join sfdc_account_xf account on account.account_id = opportunity.account_id


    ),
    eligible as (

        select
            lower(report_opportunity_user_segment) as report_opportunity_user_segment,
            lower(report_opportunity_user_geo) as report_opportunity_user_geo,
            lower(report_opportunity_user_region) as report_opportunity_user_region,
            lower(report_opportunity_user_area) as report_opportunity_user_area,

            lower(sales_qualified_source) as sales_qualified_source,
            lower(order_type_stamped) as order_type_stamped,

            lower(deal_category) as deal_category,
            lower(deal_group) as deal_group,

            lower(
                concat(
                    report_opportunity_user_segment,
                    '-',
                    report_opportunity_user_geo,
                    '-',
                    report_opportunity_user_region,
                    '-',
                    report_opportunity_user_area
                )
            ) as report_user_segment_geo_region_area,
            lower(
                concat(
                    report_opportunity_user_segment,
                    '-',
                    report_opportunity_user_geo,
                    '-',
                    report_opportunity_user_region,
                    '-',
                    report_opportunity_user_area,
                    '-',
                    sales_qualified_source,
                    '-',
                    order_type_stamped
                )
            ) as report_user_segment_geo_region_area_sqs_ot
        from sfdc_opportunity_xf

        UNION ALL

        select
            lower(account_owner_user_segment) as report_opportunity_user_segment,
            lower(account_owner_user_geo) as report_opportunity_user_geo,
            lower(account_owner_user_region) as report_opportunity_user_region,
            lower(account_owner_user_area) as report_opportunity_user_area,

            lower(sales_qualified_source) as sales_qualified_source,
            lower(order_type_stamped) as order_type_stamped,

            lower(deal_category) as deal_category,
            lower(deal_group) as deal_group,

            lower(
                concat(
                    account_owner_user_segment,
                    '-',
                    account_owner_user_geo,
                    '-',
                    account_owner_user_region,
                    '-',
                    account_owner_user_area
                )
            ) as report_user_segment_geo_region_area,
            lower(
                concat(
                    account_owner_user_segment,
                    '-',
                    account_owner_user_geo,
                    '-',
                    account_owner_user_region,
                    '-',
                    account_owner_user_area,
                    '-',
                    sales_qualified_source,
                    '-',
                    order_type_stamped
                )
            ) as report_user_segment_geo_region_area_sqs_ot
        from sfdc_opportunity_xf


    ),
    valid_keys as (

        select distinct

            -- Segment
            -- Sales Qualified Source
            -- Order Type
            -- Segment - Geo
            -- Segment - Geo - Region
            -- Segment - Geo - Order Type Group 
            -- Segment - Geo - Sales Qualified Source
            -- Segment - Geo - Region - Order Type Group 
            -- Segment - Geo - Region - Sales Qualified Source
            -- Segment - Geo - Region - Area
            -- Segment - Geo - Region - Area - Order Type Group 
            -- Segment - Geo - Region - Area - Sales Qualified Source
            eligible.*,

            report_opportunity_user_segment as key_segment,
            sales_qualified_source as key_sqs,
            deal_group as key_ot,

            report_opportunity_user_segment
            || '_'
            || sales_qualified_source
            as key_segment_sqs,
            report_opportunity_user_segment || '_' || deal_group as key_segment_ot,

            report_opportunity_user_segment
            || '_'
            || report_opportunity_user_geo
            as key_segment_geo,
            report_opportunity_user_segment
            || '_'
            || report_opportunity_user_geo
            || '_'
            || sales_qualified_source
            as key_segment_geo_sqs,
            report_opportunity_user_segment
            || '_'
            || report_opportunity_user_geo
            || '_'
            || deal_group
            as key_segment_geo_ot,


            report_opportunity_user_segment
            || '_'
            || report_opportunity_user_geo
            || '_'
            || report_opportunity_user_region
            as key_segment_geo_region,
            report_opportunity_user_segment
            || '_'
            || report_opportunity_user_geo
            || '_'
            || report_opportunity_user_region
            || '_'
            || sales_qualified_source
            as key_segment_geo_region_sqs,
            report_opportunity_user_segment
            || '_'
            || report_opportunity_user_geo
            || '_'
            || report_opportunity_user_region
            || '_'
            || deal_group
            as key_segment_geo_region_ot,

            report_opportunity_user_segment
            || '_'
            || report_opportunity_user_geo
            || '_'
            || report_opportunity_user_region
            || '_'
            || report_opportunity_user_area
            as key_segment_geo_region_area,
            report_opportunity_user_segment
            || '_'
            || report_opportunity_user_geo
            || '_'
            || report_opportunity_user_region
            || '_'
            || report_opportunity_user_area
            || '_'
            || sales_qualified_source
            as key_segment_geo_region_area_sqs,
            report_opportunity_user_segment
            || '_'
            || report_opportunity_user_geo
            || '_'
            || report_opportunity_user_region
            || '_'
            || report_opportunity_user_area
            || '_'
            || deal_group
            as key_segment_geo_region_area_ot,


            report_opportunity_user_segment
            || '_'
            || report_opportunity_user_geo
            || '_'
            || report_opportunity_user_area
            as key_segment_geo_area,

            coalesce(report_opportunity_user_segment, 'other') as sales_team_cro_level,

            -- NF: This code replicates the reporting structured of FY22, to keep
            -- current tools working
            case
                when
                    report_opportunity_user_segment = 'large'
                    and report_opportunity_user_geo = 'emea'
                then 'large_emea'
                when
                    report_opportunity_user_segment = 'mid-market'
                    and report_opportunity_user_region = 'amer'
                    and lower(report_opportunity_user_area) like '%west%'
                then 'mid-market_west'
                when
                    report_opportunity_user_segment = 'mid-market'
                    and report_opportunity_user_region = 'amer'
                    and lower(report_opportunity_user_area) not like '%west%'
                then 'mid-market_east'
                when
                    report_opportunity_user_segment = 'smb'
                    and report_opportunity_user_region = 'amer'
                    and lower(report_opportunity_user_area) like '%west%'
                then 'smb_west'
                when
                    report_opportunity_user_segment = 'smb'
                    and report_opportunity_user_region = 'amer'
                    and lower(report_opportunity_user_area) not like '%west%'
                then 'smb_east'
                when
                    report_opportunity_user_segment = 'smb'
                    and report_opportunity_user_region = 'latam'
                then 'smb_east'
                when
                    (
                        report_opportunity_user_segment is null
                        or report_opportunity_user_region is null
                    )
                then 'other'
                when
                    concat(
                        report_opportunity_user_segment,
                        '_',
                        report_opportunity_user_region
                    ) like '%other%'
                then 'other'
                else
                    concat(
                        report_opportunity_user_segment,
                        '_',
                        report_opportunity_user_region
                    )
            end as sales_team_rd_asm_level,

            coalesce(
                concat(
                    report_opportunity_user_segment, '_', report_opportunity_user_geo
                ),
                'other'
            ) as sales_team_vp_level,
            coalesce(
                concat(
                    report_opportunity_user_segment,
                    '_',
                    report_opportunity_user_geo,
                    '_',
                    report_opportunity_user_region
                ),
                'other'
            ) as sales_team_avp_rd_level,
            coalesce(
                concat(
                    report_opportunity_user_segment,
                    '_',
                    report_opportunity_user_geo,
                    '_',
                    report_opportunity_user_region,
                    '_',
                    report_opportunity_user_area
                ),
                'other'
            ) as sales_team_asm_level

        from eligible

    )

select *
from valid_keys
