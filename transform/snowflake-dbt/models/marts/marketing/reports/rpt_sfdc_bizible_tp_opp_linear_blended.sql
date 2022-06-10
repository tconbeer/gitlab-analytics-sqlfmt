{{ config(tags=["mnpi_exception"]) }}

{{ config({"schema": "common_mart_marketing"}) }}

{{
    simple_cte(
        [
            ("rpt_sfdc_bizible_linear", "rpt_sfdc_bizible_linear"),
            ("rpt_pmg_data", "rpt_pmg_data"),
            (
                "rpt_sfdc_bizible_tp_person_lifecycle",
                "rpt_sfdc_bizible_tp_person_lifecycle",
            ),
            ("dim_date", "dim_date"),
        ]
    )
}}

,
unioned as (

    select
        rpt_pmg_data.reporting_date_month_yr as bizible_touchpoint_date_month_yr,
        rpt_pmg_data.reporting_date_normalized as bizible_touchpoint_date_normalized,
        rpt_pmg_data.integrated_campaign_grouping
        as bizible_integrated_campaign_grouping,
        rpt_pmg_data.mapped_channel_source_normalized as bizible_marketing_channel_path,
        rpt_pmg_data.region_normalized as region_normalized,  -- 5       
        iff(
            rpt_pmg_data.utm_segment is null, 'Unknown', rpt_pmg_data.utm_segment
        ) as sales_segment_name,
        null as crm_person_status,
        null as bizible_touchpoint_type,
        null as sales_type,
        rpt_pmg_data.reporting_date_normalized as opp_created_date,  -- 10    
        rpt_pmg_data.reporting_date_normalized as sales_accepted_date,
        rpt_pmg_data.reporting_date_normalized as close_date,
        null as stage_name,
        null as is_won,
        null as is_sao,
        null as deal_path_name,  -- 15
        null as order_type,
        null as bizible_landing_page,
        null as bizible_form_url,
        null as dim_crm_account_id,
        null as dim_crm_opportunity_id,  -- 20
        null as crm_account_name,
        null as crm_account_gtm_strategy,
        null as country,
        rpt_pmg_data.mapped_channel as bizible_medium,
        rpt_pmg_data.touchpoint_segment,  -- 25
        rpt_pmg_data.gtm_motion,
        null as last_utm_campaign,
        null as last_utm_content,
        null as bizible_ad_campaign_name,
        null as lead_source,  -- 30
        null as campaign_type,
        rpt_pmg_data.reporting_date_normalized as mql_datetime_least,
        null as true_inquiry_date,
        null as dim_crm_person_id,
        null as is_inquiry,
        null as is_mql,
        sum(rpt_pmg_data.cost) as total_cost,
        0 as touchpoint_sum,
        0 as new_lead_created_sum,
        0 as count_true_inquiry,
        0 as mql_sum,
        0 as accepted_sum,
        0 as new_mql_sum,
        0 as new_accepted_sum,
        0 as first_opp_created,
        0 as u_shaped_opp_created,
        0 as w_shaped_opp_created,
        0 as full_shaped_opp_created,
        0 as custom_opp_created,
        0 as linear_opp_created,
        0 as first_net_arr,
        0 as u_net_arr,
        0 as w_net_arr,
        0 as full_net_arr,
        0 as custom_net_arr,
        0 as linear_net_arr,
        0 as first_sao,
        0 as u_shaped_sao,
        0 as w_shaped_sao,
        0 as full_shaped_sao,
        0 as custom_sao,
        0 as linear_sao,
        0 as pipeline_first_net_arr,
        0 as pipeline_u_net_arr,
        0 as pipeline_w_net_arr,
        0 as pipeline_full_net_arr,
        0 as pipeline_custom_net_arr,
        0 as pipeline_linear_net_arr,
        0 as won_first,
        0 as won_u,
        0 as won_w,
        0 as won_full,
        0 as won_custom,
        0 as won_linear,
        0 as won_first_net_arr,
        0 as won_u_net_arr,
        0 as won_w_net_arr,
        0 as won_full_net_arr,
        0 as won_custom_net_arr,
        0 as won_linear_net_arr
    from rpt_pmg_data {{ dbt_utils.group_by(n=37) }}
    union all
    select
        rpt_sfdc_bizible_tp_person_lifecycle.bizible_touchpoint_date_month_yr,
        rpt_sfdc_bizible_tp_person_lifecycle.bizible_touchpoint_date_normalized,
        rpt_sfdc_bizible_tp_person_lifecycle.bizible_integrated_campaign_grouping,
        rpt_sfdc_bizible_tp_person_lifecycle.bizible_marketing_channel_path,
        case
            when rpt_sfdc_bizible_tp_person_lifecycle.region = 'NORAM'
            then 'AMER'
            else rpt_sfdc_bizible_tp_person_lifecycle.region
        end as region_normalized,  -- 5
        iff(
            rpt_sfdc_bizible_tp_person_lifecycle.sales_segment_name is null,
            'Unknown',
            rpt_sfdc_bizible_tp_person_lifecycle.sales_segment_name
        ) as sales_segment_name,
        rpt_sfdc_bizible_tp_person_lifecycle.crm_person_status,
        rpt_sfdc_bizible_tp_person_lifecycle.bizible_touchpoint_type,
        null as sales_type,
        null as opp_created_date,
        null as sales_accepted_date,
        null as close_date,
        null as stage_name,
        null as is_won,  -- 15
        null as is_sao,
        null as deal_path_name,
        null as order_type,
        rpt_sfdc_bizible_tp_person_lifecycle.bizible_landing_page,
        rpt_sfdc_bizible_tp_person_lifecycle.bizible_form_url,
        rpt_sfdc_bizible_tp_person_lifecycle.dim_crm_account_id,  -- 20
        null as dim_crm_opportunity_id,
        rpt_sfdc_bizible_tp_person_lifecycle.crm_account_name as crm_account_name,
        rpt_sfdc_bizible_tp_person_lifecycle.crm_account_gtm_strategy,
        upper(rpt_sfdc_bizible_tp_person_lifecycle.person_country) as country,
        rpt_sfdc_bizible_tp_person_lifecycle.bizible_medium as bizible_medium,
        rpt_sfdc_bizible_tp_person_lifecycle.touchpoint_segment,  -- 25
        rpt_sfdc_bizible_tp_person_lifecycle.gtm_motion,
        rpt_sfdc_bizible_tp_person_lifecycle.last_utm_campaign,
        rpt_sfdc_bizible_tp_person_lifecycle.last_utm_content,
        rpt_sfdc_bizible_tp_person_lifecycle.bizible_ad_campaign_name,
        rpt_sfdc_bizible_tp_person_lifecycle.lead_source,
        rpt_sfdc_bizible_tp_person_lifecycle.campaign_type,
        rpt_sfdc_bizible_tp_person_lifecycle.mql_datetime_least::date  -- 30
        as mql_datetime_least,
        rpt_sfdc_bizible_tp_person_lifecycle.true_inquiry_date,
        rpt_sfdc_bizible_tp_person_lifecycle.dim_crm_person_id as dim_crm_person_id,
        is_inquiry,
        is_mql,
        0 as total_cost,
        sum(rpt_sfdc_bizible_tp_person_lifecycle.touchpoint_count) as touchpoint_sum,
        sum(
            rpt_sfdc_bizible_tp_person_lifecycle.bizible_count_lead_creation_touch
        ) as new_lead_created_sum,
        sum(
            rpt_sfdc_bizible_tp_person_lifecycle.count_true_inquiry
        ) as count_true_inquiry,
        sum(rpt_sfdc_bizible_tp_person_lifecycle.count_mql) as mql_sum,
        sum(rpt_sfdc_bizible_tp_person_lifecycle.count_accepted) as accepted_sum,
        sum(rpt_sfdc_bizible_tp_person_lifecycle.count_net_new_mql) as new_mql_sum,
        sum(
            rpt_sfdc_bizible_tp_person_lifecycle.count_net_new_accepted
        ) as new_accepted_sum,
        0 as first_opp_created,
        0 as u_shaped_opp_created,
        0 as w_shaped_opp_created,
        0 as full_shaped_opp_created,
        0 as custom_opp_created,
        0 as linear_opp_created,
        0 as first_net_arr,
        0 as u_net_arr,
        0 as w_net_arr,
        0 as full_net_arr,
        0 as custom_net_arr,
        0 as linear_net_arr,
        0 as first_sao,
        0 as u_shaped_sao,
        0 as w_shaped_sao,
        0 as full_shaped_sao,
        0 as custom_sao,
        0 as linear_sao,
        0 as pipeline_first_net_arr,
        0 as pipeline_u_net_arr,
        0 as pipeline_w_net_arr,
        0 as pipeline_full_net_arr,
        0 as pipeline_custom_net_arr,
        0 as pipeline_linear_net_arr,
        0 as won_first,
        0 as won_u,
        0 as won_w,
        0 as won_full,
        0 as won_custom,
        0 as won_linear,
        0 as won_first_net_arr,
        0 as won_u_net_arr,
        0 as won_w_net_arr,
        0 as won_full_net_arr,
        0 as won_custom_net_arr,
        0 as won_linear_net_arr
    from rpt_sfdc_bizible_tp_person_lifecycle {{ dbt_utils.group_by(n=37) }}
    union all
    select
        rpt_sfdc_bizible_linear.bizible_touchpoint_date_month_yr
        as opp_touchpoint_mo_yr,
        rpt_sfdc_bizible_linear.bizible_touchpoint_date_normalized
        as opp_touchpoint_date_normalized,
        rpt_sfdc_bizible_linear.bizible_integrated_campaign_grouping
        as opp_integrated_campaign_grouping,
        rpt_sfdc_bizible_linear.marketing_channel_path,
        case
            when rpt_sfdc_bizible_linear.crm_user_region = 'NORAM'
            then 'AMER'
            else rpt_sfdc_bizible_linear.crm_user_region
        end as region_normalized,  -- 5
        iff(
            rpt_sfdc_bizible_linear.crm_user_sales_segment is null,
            'Unknown',
            rpt_sfdc_bizible_linear.crm_user_sales_segment
        ) as sales_segment_name,
        null as crm_person_status,
        rpt_sfdc_bizible_linear.bizible_touchpoint_type,
        rpt_sfdc_bizible_linear.sales_type,
        rpt_sfdc_bizible_linear.opp_created_date,  -- 10
        rpt_sfdc_bizible_linear.sales_accepted_date,
        rpt_sfdc_bizible_linear.close_date,
        rpt_sfdc_bizible_linear.stage_name,
        rpt_sfdc_bizible_linear.is_won,
        rpt_sfdc_bizible_linear.is_sao,  -- 15
        rpt_sfdc_bizible_linear.deal_path_name,
        rpt_sfdc_bizible_linear.order_type as order_type,
        rpt_sfdc_bizible_linear.bizible_landing_page,
        rpt_sfdc_bizible_linear.bizible_form_url,
        rpt_sfdc_bizible_linear.dim_crm_account_id,  -- 20
        rpt_sfdc_bizible_linear.dim_crm_opportunity_id as dim_crm_opportunity_id,
        rpt_sfdc_bizible_linear.crm_account_name,
        rpt_sfdc_bizible_linear.crm_account_gtm_strategy,
        upper(rpt_sfdc_bizible_linear.country) as country,
        rpt_sfdc_bizible_linear.bizible_medium as bizible_medium,  -- 25
        rpt_sfdc_bizible_linear.touchpoint_segment,
        rpt_sfdc_bizible_linear.gtm_motion,
        rpt_sfdc_bizible_linear.last_utm_campaign,
        rpt_sfdc_bizible_linear.last_utm_content,
        rpt_sfdc_bizible_linear.bizible_ad_campaign_name,  -- 30
        rpt_sfdc_bizible_linear.lead_source,
        rpt_sfdc_bizible_linear.campaign_type,
        null as mql_datetime_least,
        null as true_inquiry_date,
        null as dim_crm_person_id,
        null as is_inquiry,
        null as is_mql,
        0 as total_cost,
        0 as touchpoint_sum,
        0 as new_lead_created_sum,
        0 as count_true_inquiry,
        0 as mql_sum,
        0 as accepted_sum,
        0 as new_mql_sum,
        0 as new_accepted_sum,
        sum(rpt_sfdc_bizible_linear.first_weight) as first_opp_created,
        sum(rpt_sfdc_bizible_linear.u_weight) as u_shaped_opp_created,
        sum(rpt_sfdc_bizible_linear.w_weight) as w_shaped_opp_created,
        sum(rpt_sfdc_bizible_linear.full_weight) as full_shaped_opp_created,
        sum(rpt_sfdc_bizible_linear.custom_weight) as custom_opp_created,
        sum(rpt_sfdc_bizible_linear.l_weight) as linear_opp_created,
        sum(rpt_sfdc_bizible_linear.first_net_arr) as first_net_arr,
        sum(rpt_sfdc_bizible_linear.u_net_arr) as u_net_arr,
        sum(rpt_sfdc_bizible_linear.w_net_arr) as w_net_arr,
        sum(rpt_sfdc_bizible_linear.full_net_arr) as full_net_arr,
        sum(rpt_sfdc_bizible_linear.custom_net_arr) as custom_net_arr,
        sum(rpt_sfdc_bizible_linear.linear_net_arr) as linear_net_arr,
        case
            when rpt_sfdc_bizible_linear.is_sao = true
            then sum(rpt_sfdc_bizible_linear.first_weight)
            else 0
        end as first_sao,
        case
            when rpt_sfdc_bizible_linear.is_sao = true
            then sum(rpt_sfdc_bizible_linear.u_weight)
            else 0
        end as u_shaped_sao,
        case
            when rpt_sfdc_bizible_linear.is_sao = true
            then sum(rpt_sfdc_bizible_linear.w_weight)
            else 0
        end as w_shaped_sao,
        case
            when rpt_sfdc_bizible_linear.is_sao = true
            then sum(rpt_sfdc_bizible_linear.full_weight)
            else 0
        end as full_shaped_sao,
        case
            when rpt_sfdc_bizible_linear.is_sao = true
            then sum(rpt_sfdc_bizible_linear.custom_weight)
            else 0
        end as custom_sao,
        case
            when rpt_sfdc_bizible_linear.is_sao = true
            then sum(rpt_sfdc_bizible_linear.l_weight)
            else 0
        end as linear_sao,
        case
            when rpt_sfdc_bizible_linear.is_sao = true
            then sum(rpt_sfdc_bizible_linear.u_net_arr)
            else 0
        end as pipeline_first_net_arr,
        case
            when rpt_sfdc_bizible_linear.is_sao = true
            then sum(rpt_sfdc_bizible_linear.u_net_arr)
            else 0
        end as pipeline_u_net_arr,
        case
            when rpt_sfdc_bizible_linear.is_sao = true
            then sum(rpt_sfdc_bizible_linear.w_net_arr)
            else 0
        end as pipeline_w_net_arr,
        case
            when rpt_sfdc_bizible_linear.is_sao = true
            then sum(rpt_sfdc_bizible_linear.full_net_arr)
            else 0
        end as pipeline_full_net_arr,
        case
            when rpt_sfdc_bizible_linear.is_sao = true
            then sum(rpt_sfdc_bizible_linear.custom_net_arr)
            else 0
        end as pipeline_custom_net_arr,
        case
            when rpt_sfdc_bizible_linear.is_sao = true
            then sum(rpt_sfdc_bizible_linear.linear_net_arr)
            else 0
        end as pipeline_linear_net_arr,
        case
            when rpt_sfdc_bizible_linear.is_won = 'True'
            then sum(rpt_sfdc_bizible_linear.first_weight)
            else 0
        end as won_first,
        case
            when rpt_sfdc_bizible_linear.is_won = 'True'
            then sum(rpt_sfdc_bizible_linear.u_weight)
            else 0
        end as won_u,
        case
            when rpt_sfdc_bizible_linear.is_won = 'True'
            then sum(rpt_sfdc_bizible_linear.w_weight)
            else 0
        end as won_w,
        case
            when rpt_sfdc_bizible_linear.is_won = 'True'
            then sum(rpt_sfdc_bizible_linear.full_weight)
            else 0
        end as won_full,
        case
            when rpt_sfdc_bizible_linear.is_won = 'True'
            then sum(rpt_sfdc_bizible_linear.custom_weight)
            else 0
        end as won_custom,
        case
            when rpt_sfdc_bizible_linear.is_won = 'True'
            then sum(rpt_sfdc_bizible_linear.l_weight)
            else 0
        end as won_linear,
        case
            when rpt_sfdc_bizible_linear.is_won = 'True'
            then sum(rpt_sfdc_bizible_linear.first_net_arr)
            else 0
        end as won_first_net_arr,
        case
            when rpt_sfdc_bizible_linear.is_won = 'True'
            then sum(rpt_sfdc_bizible_linear.u_net_arr)
            else 0
        end as won_u_net_arr,
        case
            when rpt_sfdc_bizible_linear.is_won = 'True'
            then sum(rpt_sfdc_bizible_linear.w_net_arr)
            else 0
        end as won_w_net_arr,
        case
            when rpt_sfdc_bizible_linear.is_won = 'True'
            then sum(rpt_sfdc_bizible_linear.full_net_arr)
            else 0
        end as won_full_net_arr,
        case
            when rpt_sfdc_bizible_linear.is_won = 'True'
            then sum(rpt_sfdc_bizible_linear.custom_net_arr)
            else 0
        end as won_custom_net_arr,
        case
            when rpt_sfdc_bizible_linear.is_won = 'True'
            then sum(rpt_sfdc_bizible_linear.linear_net_arr)
            else 0
        end as won_linear_net_arr
    from rpt_sfdc_bizible_linear {{ dbt_utils.group_by(n=37) }}

),
final as (

    select
        unioned.*,
        dim_date.fiscal_year as date_range_year,
        dim_date.fiscal_quarter_name_fy as date_range_quarter,
        date_trunc(month, dim_date.date_actual) as date_range_month
    from unioned
    left join
        dim_date on unioned.bizible_touchpoint_date_normalized = dim_date.date_actual
    where bizible_touchpoint_date_normalized >= '09/01/2019'

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@rkohnke",
        updated_by="@rkohnke",
        created_date="2022-01-25",
        updated_date="2022-02-02",
    )
}}
