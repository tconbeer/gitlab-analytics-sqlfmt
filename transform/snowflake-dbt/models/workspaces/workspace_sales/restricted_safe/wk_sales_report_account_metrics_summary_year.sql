{{ config(alias="report_account_metrics_summary_year") }}

with
    -- FROM  prod.workspace_sales.date_details
    date_details as (select * from {{ ref("wk_sales_date_details") }}),
    sfdc_opportunity_xf as (

        select *
        -- FROM prod.restricted_safe_workspace_sales.sfdc_opportunity_xf
        from {{ ref("wk_sales_sfdc_opportunity_xf") }}
        where is_deleted = 0 and is_edu_oss = 0 and is_jihu_account = 0

    ),
    sfdc_opportunity_snapshot_xf as (

        select h.*
        -- FROM
        -- prod.restricted_safe_workspace_sales.sfdc_opportunity_snapshot_history_xf h
        from {{ ref("wk_sales_sfdc_opportunity_snapshot_history_xf") }} h
        inner join
            date_details snapshot_date on snapshot_date.date_actual = h.snapshot_date
        where
            h.is_deleted = 0
            and h.is_edu_oss = 0
            and h.is_jihu_account = 0
            and snapshot_date.day_of_fiscal_year_normalised
            = (
                select distinct day_of_fiscal_year_normalised
                from date_details
                where date_actual = dateadd(day, -2, current_date)
            )

    ),
    stitch_subscription as (

        select
            s.id as subscription_id,
            case
                when s.invoiceownerid != s.accountid then 1 else 0
            end as is_channel_arr_flag
        -- FROM raw.zuora_stitch.subscription s 
        from {{ source("zuora", "subscription") }} s

    ),
    -- FROM prod.restricted_safe_common_mart_sales.mart_arr
    mart_arr as (select * from {{ ref("mart_arr") }}),
    -- FROM prod.restricted_safe_common.dim_crm_account
    dim_crm_account as (select * from {{ ref("dim_crm_account") }}),
    -- FROM {{ref('sfdc_accounts_xf')}} 
    sfdc_accounts_xf as (select * from prod.restricted_safe_legacy.sfdc_accounts_xf),
    -- FROM {{ source('salesforce', 'account') }}
    stitch_account as (select * from raw.salesforce_stitch.account),
    -- FROM prod.workspace_sales.sfdc_users_xf
    sfdc_users_xf as (select * from {{ ref("wk_sales_sfdc_users_xf") }}),
    report_dates as (

        select distinct
            fiscal_year as report_fiscal_year, first_day_of_month as report_month_date
        from date_details
        where fiscal_year in (2020, 2021, 2022) and month_actual = month(current_date)

    ),
    account_year_key as (

        select distinct
            a.dim_crm_account_id as account_id,
            d.report_fiscal_year,
            d.report_month_date
        from dim_crm_account a
        cross join report_dates d


    ),
    nfy_atr_base as (

        select
            o.account_id,
            -- e.g. We want to show ATR on the previous FY
            d.fiscal_year - 1 as report_fiscal_year,
            sum(o.arr_basis) as nfy_sfdc_atr
        from sfdc_opportunity_xf o
        left join date_details d on o.subscription_start_date = d.date_actual
        where
            o.sales_type = 'Renewal'
            and stage_name
            not in ('9-Unqualified', '10-Duplicate', '00-Pre Opportunity')
            and amount <> 0
        group by 1, 2

    ),
    fy_atr_base as (

        select
            o.account_id,
            -- e.g. We want to show ATR on the previous FY
            d.fiscal_year as report_fiscal_year,
            sum(o.arr_basis) as fy_sfdc_atr
        from sfdc_opportunity_xf o
        left join date_details d on o.subscription_start_date = d.date_actual
        where
            o.sales_type = 'Renewal'
            and stage_name
            not in ('9-Unqualified', '10-Duplicate', '00-Pre Opportunity')
            and amount <> 0
        group by 1, 2

    ),
    ttm_atr_base as (

        select
            o.account_id,
            -- e.g. We want to show ATR on the previous FY
            d.report_fiscal_year as report_fiscal_year,
            sum(o.arr_basis) as ttm_atr
        from sfdc_opportunity_xf o
        cross join report_dates d
        where
            o.sales_type = 'Renewal'
            and o.subscription_start_date
            between dateadd(month, -12, date_trunc('month', d.report_month_date))
            and date_trunc('month', d.report_month_date)
            and o.stage_name
            not in ('9-Unqualified', '10-Duplicate', '00-Pre Opportunity')
            and o.amount <> 0
        group by 1, 2

    -- Rolling 1 year Net ARR
    ),
    net_arr_ttm as (

        select

            o.account_id,
            d.report_fiscal_year as report_fiscal_year,
            sum(o.net_arr) as ttm_net_arr,
            sum(
                case
                    when o.sales_qualified_source != 'Web Direct Generated'
                    then o.net_arr
                    else 0
                end
            ) as ttm_non_web_net_arr,
            sum(
                case
                    when o.sales_qualified_source = 'Web Direct Generated'
                    then o.net_arr
                    else 0
                end
            ) as ttm_web_direct_sourced_net_arr,
            sum(
                case
                    when o.sales_qualified_source = 'Channel Generated'
                    then o.net_arr
                    else 0
                end
            ) as ttm_channel_sourced_net_arr,
            sum(
                case
                    when o.sales_qualified_source = 'SDR Generated'
                    then o.net_arr
                    else 0
                end
            ) as ttm_sdr_sourced_net_arr,
            sum(
                case
                    when o.sales_qualified_source = 'AE Generated' then o.net_arr else 0
                end
            ) as ttm_ae_sourced_net_arr,
            sum(
                case
                    when o.is_eligible_churn_contraction_flag = 1 then o.net_arr else 0
                end
            ) as ttm_churn_contraction_net_arr,

            -- FO year
            sum(
                case
                    when o.order_type_live = '1. New - First Order'
                    then o.net_arr
                    else 0
                end
            ) as ttm_fo_net_arr,

            -- New Connected year
            sum(
                case
                    when o.order_type_live = '2. New - Connected' then o.net_arr else 0
                end
            ) as ttm_new_connected_net_arr,

            -- Growth year
            sum(
                case
                    when
                        o.order_type_live
                        not in ('2. New - Connected', '1. New - First Order')
                    then o.net_arr
                    else 0
                end
            ) as ttm_growth_net_arr,

            -- deal path direct year
            sum(
                case when o.deal_path != 'Channel' then o.net_arr else 0 end
            ) as ttm_direct_net_arr,

            -- deal path channel year
            sum(
                case when o.deal_path = 'Channel' then o.net_arr else 0 end
            ) as ttm_channel_net_arr,

            sum(
                case when o.is_won = 1 then o.calculated_deal_count else 0 end
            ) as ttm_deal_count,

            sum(
                case
                    when (o.is_won = 1 or (o.is_renewal = 1 and o.is_lost = 1))
                    then o.calculated_deal_count
                    else 0
                end
            ) as ttm_trx_count,


            sum(
                case
                    when
                        (o.is_won = 1 or (o.is_renewal = 1 and o.is_lost = 1))
                        and (
                            (o.is_renewal = 1 and o.arr_basis > 5000)
                            or o.net_arr > 5000
                        )
                    then o.calculated_deal_count
                    else 0
                end
            ) as ttm_trx_over_5k_count,

            sum(
                case
                    when
                        (o.is_won = 1 or (o.is_renewal = 1 and o.is_lost = 1))
                        and (
                            (o.is_renewal = 1 and o.arr_basis > 10000)
                            or o.net_arr > 10000
                        )
                    then o.calculated_deal_count
                    else 0
                end
            ) as ttm_trx_over_10k_count,

            sum(
                case
                    when
                        (o.is_won = 1 or (o.is_renewal = 1 and o.is_lost = 1))
                        and (
                            (o.is_renewal = 1 and o.arr_basis > 50000)
                            or o.net_arr > 50000
                        )
                    then o.calculated_deal_count
                    else 0
                end
            ) as ttm_trx_over_50k_count,

            sum(
                case when o.is_renewal = 1 then o.calculated_deal_count else 0 end
            ) as ttm_renewal_deal_count,

            sum(
                case
                    when
                        o.is_eligible_churn_contraction_flag = 1
                        and o.opportunity_category
                        in (
                            'Standard',
                            'Internal Correction',
                            'Ramp Deal',
                            'Contract Reset',
                            'Contract Reset/Ramp Deal'
                        )
                    then o.calculated_deal_count
                    else 0
                end
            ) as ttm_churn_contraction_deal_count,

            -- deal path direct year
            sum(
                case
                    when o.deal_path != 'Channel' and o.is_won = 1
                    then o.calculated_deal_count
                    else 0
                end
            ) as ttm_direct_deal_count,

            -- deal path channel year
            sum(
                case
                    when o.deal_path = 'Channel' and o.is_won = 1
                    then o.calculated_deal_count
                    else 0
                end
            ) as ttm_channel_deal_count

        from sfdc_opportunity_xf o
        cross join report_dates d
        where
            o.close_date
            between dateadd(month, -12, date_trunc('month', d.report_month_date)) and
            date_trunc('month', d.report_month_date)
            and (
                o.stage_name = 'Closed Won'
                or (o.sales_type = 'Renewal' and o.stage_name = '8-Closed Lost')
            )
            and o.net_arr <> 0
        group by 1, 2

    -- total booked net arr in fy
    ),
    total_net_arr_fiscal as (

        select
            o.account_id,
            o.close_fiscal_year as report_fiscal_year,
            sum(o.net_arr) as fy_net_arr,
            sum(
                case
                    when o.sales_qualified_source != 'Web Direct Generated'
                    then o.net_arr
                    else 0
                end
            ) as fy_non_web_booked_net_arr,
            sum(
                case
                    when o.sales_qualified_source = 'Web Direct Generated'
                    then o.net_arr
                    else 0
                end
            ) as fy_web_direct_sourced_net_arr,
            sum(
                case
                    when o.sales_qualified_source = 'Channel Generated'
                    then o.net_arr
                    else 0
                end
            ) as fy_channel_sourced_net_arr,
            sum(
                case
                    when o.sales_qualified_source = 'SDR Generated'
                    then o.net_arr
                    else 0
                end
            ) as fy_sdr_sourced_net_arr,
            sum(
                case
                    when o.sales_qualified_source = 'AE Generated' then o.net_arr else 0
                end
            ) as fy_ae_sourced_net_arr,
            sum(
                case
                    when o.is_eligible_churn_contraction_flag = 1 then o.net_arr else 0
                end
            ) as fy_churn_contraction_net_arr,

            -- First Order year
            sum(
                case
                    when o.order_type_live = '1. New - First Order'
                    then o.net_arr
                    else 0
                end
            ) as fy_fo_net_arr,

            -- New Connected year
            sum(
                case
                    when o.order_type_live = '2. New - Connected' then o.net_arr else 0
                end
            ) as fy_new_connected_net_arr,

            -- Growth year
            sum(
                case
                    when
                        o.order_type_live
                        not in ('2. New - Connected', '1. New - First Order')
                    then o.net_arr
                    else 0
                end
            ) as fy_growth_net_arr,

            sum(o.calculated_deal_count) as fy_deal_count,

            -- deal path direct year
            sum(
                case when o.deal_path != 'Channel' then o.net_arr else 0 end
            ) as fy_direct_net_arr,

            -- deal path channel year
            sum(
                case when o.deal_path = 'Channel' then o.net_arr else 0 end
            ) as fy_channel_net_arr,

            -- deal path direct year
            sum(
                case
                    when o.deal_path != 'Channel' then o.calculated_deal_count else 0
                end
            ) as fy_direct_deal_count,

            -- deal path channel year
            sum(
                case
                    when o.deal_path = 'Channel' then o.calculated_deal_count else 0
                end
            ) as fy_channel_deal_count

        from sfdc_opportunity_xf o
        where
            (
                o.stage_name = 'Closed Won'
                or (o.sales_type = 'Renewal' and o.stage_name = '8-Closed Lost')
            )
            and o.net_arr <> 0
        group by 1, 2

    -- Total open pipeline at the same point in previous fiscal years (total open pipe)
    ),
    op_forward_one_year as (

        select
            h.account_id,
            h.snapshot_fiscal_year as report_fiscal_year,
            sum(h.net_arr) as open_pipe,
            sum(h.calculated_deal_count) as count_open_deals
        from sfdc_opportunity_snapshot_xf h
        where
            h.close_date > h.snapshot_date
            and h.forecast_category_name not in ('Omitted', 'Closed')
            and h.stage_name
            in (
                '1-Discovery',
                '2-Scoping',
                '3-Technical',
                'Evaluation',
                '4-Proposal',
                '5-Negotiating',
                '6-Awaiting Signature',
                '7-Closing'
            )
            and h.order_type_stamped != '7. PS / Other'
            and h.net_arr != 0
            and h.is_eligible_open_pipeline_flag = 1
        group by 1, 2

    -- Last 12 months pipe gen at same point of time in the year
    ),
    pg_last_12_months as (

        select
            h.account_id,
            h.snapshot_fiscal_year as report_fiscal_year,
            sum(h.net_arr) as pg_last_12m_net_arr,
            sum(
                case
                    when h.sales_qualified_source = 'Web Direct Generated'
                    then h.net_arr
                    else 0
                end
            ) as pg_last_12m_web_direct_sourced_net_arr,
            sum(
                case
                    when h.sales_qualified_source = 'Channel Generated'
                    then h.net_arr
                    else 0
                end
            ) as pg_last_12m_channel_sourced_net_arr,
            sum(
                case
                    when h.sales_qualified_source = 'SDR Generated'
                    then h.net_arr
                    else 0
                end
            ) as pg_last_12m_sdr_sourced_net_arr,
            sum(
                case
                    when h.sales_qualified_source = 'AE Generated' then h.net_arr else 0
                end
            ) as pg_last_12m_ae_sourced_net_arr,

            sum(
                case
                    when h.sales_qualified_source = 'Web Direct Generated'
                    then h.calculated_deal_count
                    else 0
                end
            ) as pg_last_12m_web_direct_sourced_deal_count,
            sum(
                case
                    when h.sales_qualified_source = 'Channel Generated'
                    then h.calculated_deal_count
                    else 0
                end
            ) as pg_last_12m_channel_sourced_deal_count,
            sum(
                case
                    when h.sales_qualified_source = 'SDR Generated'
                    then h.calculated_deal_count
                    else 0
                end
            ) as pg_last_12m_sdr_sourced_deal_count,
            sum(
                case
                    when h.sales_qualified_source = 'AE Generated'
                    then h.calculated_deal_count
                    else 0
                end
            ) as pg_last_12m_ae_sourced_deal_count



        from sfdc_opportunity_snapshot_xf h
        -- pipeline created within the last 12 months
        where
            h.pipeline_created_date > dateadd(month, -12, h.snapshot_date)
            and h.pipeline_created_date <= h.snapshot_date
            and h.stage_name
            in (
                '1-Discovery',
                '2-Scoping',
                '3-Technical',
                'Evaluation',
                '4-Proposal',
                '5-Negotiating',
                '6-Awaiting Signature',
                '7-Closing',
                'Closed Won',
                '8-Closed Lost'
            )
            and h.order_type_stamped != '7. PS / Other'
            and h.is_eligible_created_pipeline_flag = 1
        group by 1, 2

    -- Pipe generation at the same point in time in the fiscal year
    ),
    pg_ytd as (

        select
            h.account_id,
            h.net_arr_created_fiscal_year as report_fiscal_year,
            sum(h.net_arr) as pg_ytd_net_arr,
            sum(
                case
                    when h.sales_qualified_source = 'Web Direct Generated'
                    then h.net_arr
                    else 0
                end
            ) as pg_ytd_web_direct_sourced_net_arr,
            sum(
                case
                    when h.sales_qualified_source = 'Channel Generated'
                    then h.net_arr
                    else 0
                end
            ) as pg_ytd_channel_sourced_net_arr,
            sum(
                case
                    when h.sales_qualified_source = 'SDR Generated'
                    then h.net_arr
                    else 0
                end
            ) as pg_ytd_sdr_sourced_net_arr,
            sum(
                case
                    when h.sales_qualified_source = 'AE Generated' then h.net_arr else 0
                end
            ) as pg_ytd_ae_sourced_net_arr
        from sfdc_opportunity_snapshot_xf h
        -- pipeline created within the fiscal year
        where
            h.snapshot_fiscal_year = h.net_arr_created_fiscal_year
            and h.stage_name
            in (
                '1-Discovery',
                '2-Scoping',
                '3-Technical',
                'Evaluation',
                '4-Proposal',
                '5-Negotiating',
                '6-Awaiting Signature',
                '7-Closing',
                'Closed Won',
                '8-Closed Lost'
            )
            and h.order_type_stamped != '7. PS / Other'
            and h.is_eligible_created_pipeline_flag = 1
            and h.net_arr > 0
        group by 1, 2

    -- ARR at the same point in time in Fiscal Year
    ),
    arr_at_same_month as (

        select
            mrr.dim_crm_account_id as account_id,
            mrr_date.fiscal_year as report_fiscal_year,
            -- ultimate_parent_account_id,
            sum(mrr.mrr) as mrr,
            sum(mrr.arr) as arr,
            sum(
                case when sub.is_channel_arr_flag = 1 then mrr.arr else 0 end
            ) as reseller_arr,
            sum(
                case when sub.is_channel_arr_flag = 0 then mrr.arr else 0 end
            ) as direct_arr,


            sum(
                case
                    when
                        (
                            mrr.product_tier_name like '%Starter%'
                            or mrr.product_tier_name like '%Bronze%'
                        )
                    then mrr.arr
                    else 0
                end
            ) as product_starter_arr,


            sum(
                case when mrr.product_tier_name like '%Premium%' then mrr.arr else 0 end
            ) as product_premium_arr,
            sum(
                case
                    when mrr.product_tier_name like '%Ultimate%' then mrr.arr else 0
                end
            ) as product_ultimate_arr,

            sum(
                case
                    when mrr.product_tier_name like '%Self-Managed%' then mrr.arr else 0
                end
            ) as delivery_self_managed_arr,
            sum(
                case when mrr.product_tier_name like '%SaaS%' then mrr.arr else 0 end
            ) as delivery_saas_arr

        from mart_arr mrr
        inner join date_details mrr_date on mrr.arr_month = mrr_date.date_actual
        inner join
            stitch_subscription sub on sub.subscription_id = mrr.dim_subscription_id
        where
            mrr_date.month_actual
            = (
                select distinct month_actual
                from date_details
                where
                    date_actual = date_trunc('month', dateadd(month, -1, current_date))
            )
        group by 1, 2

    ),
    country as (

        select distinct billingcountry as countryname, billingcountrycode as countrycode
        from stitch_account

    ),
    consolidated_accounts as (

        select
            ak.report_fiscal_year,
            a.account_id as account_id,
            a.account_name as account_name,
            a.ultimate_parent_account_id as upa_id,
            a.ultimate_parent_account_name as upa_name,
            u.name as account_owner_name,
            a.owner_id as account_owner_id,
            -- u.start_date                      AS account_owner_start_date, 
            trim(u.employee_number) as account_owner_employee_number,
            -- LEAST(12,datediff(month,u.start_date,ak.report_month_date)) AS
            -- account_months_in_year,
            upa_owner.name as upa_owner_name,
            upa_owner.user_id as upa_owner_id,
            -- upa_owner.start_date            AS upa_owner_start_date,
            trim(upa_owner.employee_number) as upa_owner_employee_number,
            -- LEAST(12,datediff(month,upa_owner.start_date,ak.report_month_date)) AS
            -- upa_months_in_year,
            raw.forbes_2000_rank__c as account_forbes_rank,
            a.billing_country as account_country,
            coalesce(
                upa_c.countryname,
                replace(
                    replace(
                        replace(
                            upa.tsp_address_country, 'The Netherlands', 'Netherlands'
                        ),
                        'Russian Federation',
                        'Russia'
                    ),
                    'Russia',
                    'Russian Federation'
                )
            ) as upa_country,
            uparaw.account_demographics_upa_state__c as upa_state,
            uparaw.account_demographics_upa_city__c as upa_city,
            uparaw.account_demographics_upa_postal_code__c as upa_zip_code,
            u.user_geo as account_user_geo,
            u.user_region as account_user_region,
            u.user_segment as account_user_segment,
            u.user_area as account_user_area,
            u.role_name as account_owner_role,
            a.industry as account_industry,
            upa_owner.user_geo as upa_user_geo,
            upa_owner.user_region as upa_user_region,
            upa_owner.user_segment as upa_user_segment,
            upa_owner.user_area as upa_user_area,
            upa_owner.role_name as upa_user_role,
            upa.industry as upa_industry,
            coalesce(raw.potential_users__c, 0) as potential_users,
            coalesce(raw.number_of_licenses_this_account__c, 0) as licenses,
            coalesce(raw.decision_maker_count_linkedin__c, 0) as linkedin_developer,
            coalesce(raw.zi_number_of_developers__c, 0) as zi_developers,
            coalesce(raw.zi_revenue__c, 0) as zi_revenue,
            coalesce(raw.account_demographics_employee_count__c, 0) as employees,
            coalesce(raw.aggregate_developer_count__c, 0) as upa_aggregate_dev_count,
            least(
                50000,
                greatest(
                    coalesce(raw.number_of_licenses_this_account__c, 0),
                    coalesce(
                        raw.potential_users__c,
                        raw.decision_maker_count_linkedin__c,
                        raw.zi_number_of_developers__c,
                        raw.zi_number_of_developers__c,
                        0
                    )
                )
            ) as calculated_developer_count,

            a.technical_account_manager_date,
            a.technical_account_manager as technical_account_manager_name,

            case
                when a.technical_account_manager is not null
                then 1
                else 0
            end as has_technical_account_manager_flag,

            a.health_score_color as account_health_score_color,
            a.health_number as account_health_number,

            -- LAM
            -- COALESCE(raw.potential_arr_lam__c,0)            AS potential_arr_lam,
            -- COALESCE(raw.potential_carr_this_account__c,0)  AS
            -- potential_carr_this_account,
            -- atr for current fy
            coalesce(fy_atr_base.fy_sfdc_atr, 0) as fy_sfdc_atr,
            -- next fiscal year atr base reported at fy
            coalesce(nfy_atr_base.nfy_sfdc_atr, 0) as nfy_sfdc_atr,
            -- last 12 months ATR
            coalesce(ttm_atr.ttm_atr, 0) as ttm_atr,

            -- arr by fy
            coalesce(arr.arr, 0) as arr,

            coalesce(arr.reseller_arr, 0) as arr_channel,
            coalesce(arr.direct_arr, 0) as arr_direct,

            coalesce(arr.product_starter_arr, 0) as product_starter_arr,
            coalesce(arr.product_premium_arr, 0) as product_premium_arr,
            coalesce(arr.product_ultimate_arr, 0) as product_ultimate_arr,


            case
                when
                    coalesce(arr.product_ultimate_arr, 0)
                    > coalesce(arr.product_starter_arr, 0)
                    + coalesce(arr.product_premium_arr, 0)
                then 1
                else 0
            end as is_ultimate_customer_flag,

            case
                when
                    coalesce(arr.product_ultimate_arr, 0)
                    < coalesce(arr.product_starter_arr, 0)
                    + coalesce(arr.product_premium_arr, 0)
                then 1
                else 0
            end as is_premium_customer_flag,

            coalesce(arr.delivery_self_managed_arr, 0) as delivery_self_managed_arr,
            coalesce(arr.delivery_saas_arr, 0) as delivery_saas_arr,


            -- accounts counts
            case when coalesce(arr.arr, 0) = 0 then 1 else 0 end as is_prospect_flag,

            case when coalesce(arr.arr, 0) > 0 then 1 else 0 end as is_customer_flag,

            case
                when coalesce(arr.arr, 0) > 5000 then 1 else 0
            end as is_over_5k_customer_flag,
            case
                when coalesce(arr.arr, 0) > 10000 then 1 else 0
            end as is_over_10k_customer_flag,
            case
                when coalesce(arr.arr, 0) > 50000
                then 1
                else 0
            end as is_over_50k_customer_flag,

            case
                when coalesce(arr.arr, 0) > 100000
                then 1
                else 0
            end as is_over_100k_customer_flag,

            case
                when coalesce(arr.arr, 0) > 500000
                then 1
                else 0
            end as is_over_500k_customer_flag,

            -- rolling last 12 months bokked net arr
            coalesce(net_arr_ttm.ttm_net_arr, 0) as ttm_net_arr,
            coalesce(net_arr_ttm.ttm_non_web_net_arr, 0) as ttm_non_web_net_arr,
            coalesce(
                net_arr_ttm.ttm_web_direct_sourced_net_arr, 0
            ) as ttm_web_direct_sourced_net_arr,
            coalesce(
                net_arr_ttm.ttm_channel_sourced_net_arr, 0
            ) as ttm_channel_sourced_net_arr,
            coalesce(net_arr_ttm.ttm_sdr_sourced_net_arr, 0) as ttm_sdr_sourced_net_arr,
            coalesce(net_arr_ttm.ttm_ae_sourced_net_arr, 0) as ttm_ae_sourced_net_arr,
            coalesce(
                net_arr_ttm.ttm_churn_contraction_net_arr, 0
            ) as ttm_churn_contraction_net_arr,
            coalesce(net_arr_ttm.ttm_fo_net_arr, 0) as ttm_fo_net_arr,
            coalesce(
                net_arr_ttm.ttm_new_connected_net_arr, 0
            ) as ttm_new_connected_net_arr,
            coalesce(net_arr_ttm.ttm_growth_net_arr, 0) as ttm_growth_net_arr,
            coalesce(net_arr_ttm.ttm_deal_count, 0) as ttm_deal_count,
            coalesce(net_arr_ttm.ttm_direct_net_arr, 0) as ttm_direct_net_arr,
            coalesce(net_arr_ttm.ttm_channel_net_arr, 0) as ttm_channel_net_arr,
            coalesce(net_arr_ttm.ttm_channel_net_arr, 0)
            - coalesce(
                net_arr_ttm.ttm_channel_sourced_net_arr, 0
            ) as ttm_channel_co_sell_net_arr,
            coalesce(net_arr_ttm.ttm_direct_deal_count, 0) as ttm_direct_deal_count,
            coalesce(net_arr_ttm.ttm_channel_deal_count, 0) as ttm_channel_deal_count,
            coalesce(
                net_arr_ttm.ttm_churn_contraction_deal_count, 0
            ) as ttm_churn_contraction_deal_count,
            coalesce(net_arr_ttm.ttm_renewal_deal_count, 0) as ttm_renewal_deal_count,

            coalesce(net_arr_ttm.ttm_trx_count, 0) as ttm_trx_count,
            coalesce(net_arr_ttm.ttm_trx_over_10k_count, 0) as ttm_trx_over_5k_count,
            coalesce(net_arr_ttm.ttm_trx_over_10k_count, 0) as ttm_trx_over_10k_count,
            coalesce(net_arr_ttm.ttm_trx_over_50k_count, 0) as ttm_trx_over_50k_count,

            -- fy booked net arr
            coalesce(net_arr_fiscal.fy_net_arr, 0) as fy_net_arr,
            coalesce(
                net_arr_fiscal.fy_web_direct_sourced_net_arr, 0
            ) as fy_web_direct_sourced_net_arr,
            coalesce(
                net_arr_fiscal.fy_channel_sourced_net_arr, 0
            ) as fy_channel_sourced_net_arr,
            coalesce(
                net_arr_fiscal.fy_sdr_sourced_net_arr, 0
            ) as fy_sdr_sourced_net_arr,
            coalesce(net_arr_fiscal.fy_ae_sourced_net_arr, 0) as fy_ae_sourced_net_arr,
            coalesce(
                net_arr_fiscal.fy_churn_contraction_net_arr, 0
            ) as fy_churn_contraction_net_arr,
            coalesce(net_arr_fiscal.fy_fo_net_arr, 0) as fy_fo_net_arr,
            coalesce(
                net_arr_fiscal.fy_new_connected_net_arr, 0
            ) as fy_new_connected_net_arr,
            coalesce(net_arr_fiscal.fy_growth_net_arr, 0) as fy_growth_net_arr,
            coalesce(net_arr_fiscal.fy_deal_count, 0) as fy_deal_count,
            coalesce(net_arr_fiscal.fy_direct_net_arr, 0) as fy_direct_net_arr,
            coalesce(net_arr_fiscal.fy_channel_net_arr, 0) as fy_channel_net_arr,
            coalesce(net_arr_fiscal.fy_direct_deal_count, 0) as fy_direct_deal_count,
            coalesce(net_arr_fiscal.fy_channel_deal_count, 0) as fy_channel_deal_count,

            -- open pipe forward looking
            coalesce(op.open_pipe, 0) as open_pipe,
            coalesce(op.count_open_deals, 0) as count_open_deals_pipe,

            case
                when coalesce(arr.arr, 0) > 0 and coalesce(op.open_pipe, 0) > 0
                then 1
                else 0
            end as customer_has_open_pipe_flag,

            case
                when coalesce(arr.arr, 0) = 0 and coalesce(op.open_pipe, 0) > 0
                then 1
                else 0
            end as prospect_has_open_pipe_flag,


            -- pipe generation
            coalesce(pg.pg_ytd_net_arr, 0) as pg_ytd_net_arr,
            coalesce(
                pg.pg_ytd_web_direct_sourced_net_arr, 0
            ) as pg_ytd_web_direct_sourced_net_arr,
            coalesce(
                pg.pg_ytd_channel_sourced_net_arr, 0
            ) as pg_ytd_channel_sourced_net_arr,
            coalesce(pg.pg_ytd_sdr_sourced_net_arr, 0) as pg_ytd_sdr_sourced_net_arr,
            coalesce(pg.pg_ytd_ae_sourced_net_arr, 0) as pg_ytd_ae_sourced_net_arr,

            coalesce(pg_ly.pg_last_12m_net_arr, 0) as pg_last_12m_net_arr,
            coalesce(
                pg_ly.pg_last_12m_web_direct_sourced_net_arr, 0
            ) as pg_last_12m_web_direct_sourced_net_arr,
            coalesce(
                pg_ly.pg_last_12m_channel_sourced_net_arr, 0
            ) as pg_last_12m_channel_sourced_net_arr,
            coalesce(
                pg_ly.pg_last_12m_sdr_sourced_net_arr, 0
            ) as pg_last_12m_sdr_sourced_net_arr,
            coalesce(
                pg_ly.pg_last_12m_ae_sourced_net_arr,
                0
            ) as pg_last_12m_ae_sourced_net_arr,


            coalesce(
                pg_last_12m_web_direct_sourced_deal_count, 0
            ) as pg_last_12m_web_direct_sourced_deal_count,
            coalesce(
                pg_last_12m_channel_sourced_deal_count, 0
            ) as pg_last_12m_channel_sourced_deal_count,
            coalesce(
                pg_last_12m_sdr_sourced_deal_count, 0
            ) as pg_last_12m_sdr_sourced_deal_count,
            coalesce(
                pg_last_12m_ae_sourced_deal_count,
                0
            ) as pg_last_12m_ae_sourced_deal_count


        from account_year_key ak
        inner join sfdc_accounts_xf a on ak.account_id = a.account_id
        left join stitch_account raw on ak.account_id = raw.account_id_18__c
        left join
            stitch_account uparaw
            on a.ultimate_parent_account_id = uparaw.account_id_18__c
        left join
            country upa_c
            on uparaw.account_demographics_upa_country__c = upa_c.countrycode
        left join sfdc_users_xf u on a.owner_id = u.user_id
        left join sfdc_accounts_xf upa on a.ultimate_parent_account_id = upa.account_id
        left join sfdc_users_xf upa_owner on upa.owner_id = upa_owner.user_id
        left join
            fy_atr_base
            on a.account_id = fy_atr_base.account_id
            and fy_atr_base.report_fiscal_year = ak.report_fiscal_year
        left join
            ttm_atr_base ttm_atr
            on a.account_id = ttm_atr.account_id
            and ttm_atr.report_fiscal_year = ak.report_fiscal_year
        left join
            nfy_atr_base
            on a.account_id = nfy_atr_base.account_id
            and nfy_atr_base.report_fiscal_year = ak.report_fiscal_year
        left join
            net_arr_ttm
            on a.account_id = net_arr_ttm.account_id
            and net_arr_ttm.report_fiscal_year = ak.report_fiscal_year
        left join
            op_forward_one_year op
            on a.account_id = op.account_id
            and op.report_fiscal_year = ak.report_fiscal_year
        left join
            pg_ytd pg
            on a.account_id = pg.account_id
            and pg.report_fiscal_year = ak.report_fiscal_year
        left join
            pg_last_12_months pg_ly
            on a.account_id = pg_ly.account_id
            and pg_ly.report_fiscal_year = ak.report_fiscal_year
        left join
            arr_at_same_month arr
            on a.account_id = arr.account_id
            and arr.report_fiscal_year = ak.report_fiscal_year
        left join
            total_net_arr_fiscal net_arr_fiscal
            on a.account_id = net_arr_fiscal.account_id
            and net_arr_fiscal.report_fiscal_year = ak.report_fiscal_year

    ),
    consolidated_upa as (

        select
            report_fiscal_year,
            upa_id,
            upa_name,
            upa_owner_name,
            upa_owner_id,
            upa_country,
            upa_state,
            upa_city,
            upa_zip_code,
            upa_user_geo,
            upa_user_region,
            upa_user_segment,
            upa_user_area,
            upa_user_role,
            upa_industry,
            sum(
                case when account_forbes_rank is not null then 1 else 0 end
            ) as count_forbes_accounts,
            min(account_forbes_rank) as forbes_rank,
            max(potential_users) as potential_users,
            max(licenses) as licenses,
            max(linkedin_developer) as linkedin_developer,
            max(zi_developers) as zi_developers,
            max(zi_revenue) as zi_revenue,
            max(employees) as employees,
            max(upa_aggregate_dev_count) as upa_aggregate_dev_count,

            sum(has_technical_account_manager_flag) as count_technical_account_managers,

            -- LAM
            -- MAX(potential_arr_lam)            AS potential_arr_lam,
            -- MAX(potential_carr_this_account)  AS potential_carr_this_account,
            -- atr for current fy
            sum(fy_sfdc_atr) as fy_sfdc_atr,
            -- next fiscal year atr base reported at fy
            sum(nfy_sfdc_atr) as nfy_sfdc_atr,

            -- arr by fy
            sum(arr) as arr,

            max(is_customer_flag) as is_customer_flag,
            max(is_over_5k_customer_flag) as is_over_5k_customer_flag,
            max(is_over_10k_customer_flag) as is_over_10k_customer_flag,
            max(is_over_50k_customer_flag) as is_over_50k_customer_flag,
            max(is_over_500k_customer_flag) as is_over_500k_customer_flag,
            sum(is_over_5k_customer_flag) as count_over_5k_customers,
            sum(is_over_10k_customer_flag) as count_over_10k_customers,
            sum(is_over_50k_customer_flag) as count_over_50k_customers,
            sum(is_over_500k_customer_flag) as count_over_500k_customers,
            sum(is_prospect_flag) as count_of_prospects,
            sum(is_customer_flag) as count_of_customers,

            sum(arr_channel) as arr_channel,
            sum(arr_direct) as arr_direct,

            sum(product_starter_arr) as product_starter_arr,
            sum(product_premium_arr) as product_premium_arr,
            sum(product_ultimate_arr) as product_ultimate_arr,
            sum(delivery_self_managed_arr) as delivery_self_managed_arr,
            sum(delivery_saas_arr) as delivery_saas_arr,


            -- rolling last 12 months bokked net arr
            sum(ttm_net_arr) as ttm_net_arr,
            sum(ttm_non_web_net_arr) as ttm_non_web_net_arr,
            sum(ttm_web_direct_sourced_net_arr) as ttm_web_direct_sourced_net_arr,
            sum(ttm_channel_sourced_net_arr) as ttm_channel_sourced_net_arr,
            sum(ttm_sdr_sourced_net_arr) as ttm_sdr_sourced_net_arr,
            sum(ttm_ae_sourced_net_arr) as ttm_ae_sourced_net_arr,
            sum(ttm_churn_contraction_net_arr) as ttm_churn_contraction_net_arr,
            sum(ttm_fo_net_arr) as ttm_fo_net_arr,
            sum(ttm_new_connected_net_arr) as ttm_new_connected_net_arr,
            sum(ttm_growth_net_arr) as ttm_growth_net_arr,
            sum(ttm_deal_count) as ttm_deal_count,
            sum(ttm_direct_net_arr) as ttm_direct_net_arr,
            sum(ttm_channel_net_arr) as ttm_channel_net_arr,
            sum(ttm_atr) as ttm_atr,

            -- fy booked net arr
            sum(fy_net_arr) as fy_net_arr,
            sum(fy_web_direct_sourced_net_arr) as fy_web_direct_sourced_net_arr,
            sum(fy_channel_sourced_net_arr) as fy_channel_sourced_net_arr,
            sum(fy_sdr_sourced_net_arr) as fy_sdr_sourced_net_arr,
            sum(fy_ae_sourced_net_arr) as fy_ae_sourced_net_arr,
            sum(fy_churn_contraction_net_arr) as fy_churn_contraction_net_arr,
            sum(fy_fo_net_arr) as fy_fo_net_arr,
            sum(fy_new_connected_net_arr) as fy_new_connected_net_arr,
            sum(fy_growth_net_arr) as fy_growth_net_arr,
            sum(fy_deal_count) as fy_deal_count,
            sum(fy_direct_net_arr) as fy_direct_net_arr,
            sum(fy_channel_net_arr) as fy_channel_net_arr,
            sum(fy_direct_deal_count) as fy_direct_deal_count,
            sum(fy_channel_deal_count) as fy_channel_deal_count,

            -- open pipe forward looking
            sum(open_pipe) as open_pipe,
            sum(count_open_deals_pipe) as count_open_deals_pipe,
            sum(customer_has_open_pipe_flag) as customer_has_open_pipe_flag,
            sum(prospect_has_open_pipe_flag) as prospect_has_open_pipe_flag,

            -- pipe generation
            sum(pg_ytd_net_arr) as pg_ytd_net_arr,
            sum(pg_ytd_web_direct_sourced_net_arr) as pg_ytd_web_direct_sourced_net_arr,
            sum(pg_ytd_channel_sourced_net_arr) as pg_ytd_channel_sourced_net_arr,
            sum(pg_ytd_sdr_sourced_net_arr) as pg_ytd_sdr_sourced_net_arr,
            sum(pg_ytd_ae_sourced_net_arr) as pg_ytd_ae_sourced_net_arr,

            sum(pg_last_12m_net_arr) as pg_last_12m_net_arr,
            sum(
                pg_last_12m_web_direct_sourced_net_arr
            ) as pg_last_12m_web_direct_sourced_net_arr,
            sum(
                pg_last_12m_channel_sourced_net_arr
            ) as pg_last_12m_channel_sourced_net_arr,
            sum(pg_last_12m_sdr_sourced_net_arr) as pg_last_12m_sdr_sourced_net_arr,
            sum(pg_last_12m_ae_sourced_net_arr) as pg_last_12m_ae_sourced_net_arr


        from consolidated_accounts
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15

    ),
    upa_lam as (

        select
            upa_id,

            arr,

            case
                when potential_users > licenses
                then potential_users
                else 0
            end as adjusted_potential_users,

            case
                when linkedin_developer > licenses
                then linkedin_developer
                else 0
            end as adjusted_linkedin_developers,

            case
                when zi_developers > licenses
                then zi_developers
                else 0
            end as adjusted_zi_developers,

            case
                when employees * 0.1 > licenses
                then round(employees * 0.1, 0)
                else 0
            end as adjusted_employees,

            is_customer_flag,

            -- LEAST(50000,GREATEST(licenses,
            -- COALESCE(adjusted_potential_users,adjusted_linkedin_developers,adjusted_zi_developers,adjusted_employees))) AS lam_dev_count
            upa_aggregate_dev_count as lam_dev_count

        from consolidated_upa
        where report_fiscal_year = 2022


    ),
    final as (

        select
            acc.*,
            case
                when acc.calculated_developer_count > 500 then 1 else 0
            end as account_has_over_500_dev_flag,
            case when upa.upa_id = acc.account_id then upa.arr else 0 end as upa_arr,

            case
                when upa.upa_id = acc.account_id
                then upa.adjusted_potential_users
                else 0
            end as upa_potential_users,
            case
                when upa.upa_id = acc.account_id
                then upa.adjusted_linkedin_developers
                else 0
            end as upa_linkedin_developers,
            case
                when upa.upa_id = acc.account_id then upa.adjusted_zi_developers else 0
            end as upa_zi_developers,
            case
                when upa.upa_id = acc.account_id then upa.adjusted_employees else 0
            end as upa_employees,
            case
                when upa.upa_id = acc.account_id
                then coalesce(upa.lam_dev_count, 0)
                else 0
            end as lam_dev_count,
            case when upa.upa_id = acc.account_id then 1 else 0 end as is_upa_flag,

            upa.is_customer_flag as hierarchy_is_customer_flag,
            case
                when coalesce(upa.lam_dev_count, 0) > 500 then 1 else 0
            end as hierarchy_has_over_500_dev_flag
        from consolidated_accounts acc
        left join upa_lam upa on upa.upa_id = acc.upa_id
        where acc.report_fiscal_year = 2022

    )

select *
from final
