{{ config(tags=["mnpi_exception"]) }}

{{ config({"schema": "common_mart_marketing"}) }}

{{
    simple_cte(
        [("mart_crm_attribution_touchpoint", "mart_crm_attribution_touchpoint")]
    )
}},
linear_base as (  -- the number of touches a given opp has in total
    -- linear attribution Net_Arr of an opp / all touches (count_touches) for each opp
    -- - weighted by the number of touches in the given bucket (campaign,channel,etc)
    select
        dim_crm_opportunity_id,
        net_arr,
        count(
            distinct mart_crm_attribution_touchpoint.dim_crm_touchpoint_id
        ) as count_touches,
        net_arr / count_touches as weighted_linear_net_arr
    from mart_crm_attribution_touchpoint
    group by 1, 2

),
campaigns_per_opp as (

    select
        dim_crm_opportunity_id,
        count(
            distinct mart_crm_attribution_touchpoint.dim_campaign_id
        ) as campaigns_per_opp
    from mart_crm_attribution_touchpoint
    group by 1

),
final as (

    select
        mart_crm_attribution_touchpoint.dim_crm_opportunity_id,
        mart_crm_attribution_touchpoint.dim_crm_touchpoint_id,
        mart_crm_attribution_touchpoint.dim_campaign_id,
        mart_crm_attribution_touchpoint.sfdc_record_id,
        coalesce(
            mart_crm_attribution_touchpoint.crm_account_billing_country,
            mart_crm_attribution_touchpoint.crm_person_country
        ) as country,  -- 5
        mart_crm_attribution_touchpoint.crm_person_title,
        mart_crm_attribution_touchpoint.bizible_salesforce_campaign,
        mart_crm_attribution_touchpoint.campaign_name,
        mart_crm_attribution_touchpoint.inquiry_date,
        mart_crm_attribution_touchpoint.opportunity_close_date,
        mart_crm_attribution_touchpoint.net_arr,
        mart_crm_attribution_touchpoint.dim_crm_account_id,
        mart_crm_attribution_touchpoint.crm_account_name,
        mart_crm_attribution_touchpoint.crm_account_gtm_strategy,
        (
            mart_crm_attribution_touchpoint.net_arr
            / campaigns_per_opp.campaigns_per_opp
        ) as net_arr_per_campaign,
        linear_base.count_touches,
        mart_crm_attribution_touchpoint.bizible_touchpoint_date,
        mart_crm_attribution_touchpoint.bizible_touchpoint_position,
        mart_crm_attribution_touchpoint.bizible_touchpoint_source,
        mart_crm_attribution_touchpoint.bizible_touchpoint_type,
        mart_crm_attribution_touchpoint.bizible_ad_campaign_name,
        mart_crm_attribution_touchpoint.bizible_ad_content,
        mart_crm_attribution_touchpoint.bizible_form_url_raw,
        mart_crm_attribution_touchpoint.bizible_landing_page_raw,
        mart_crm_attribution_touchpoint.bizible_referrer_page_raw,
        mart_crm_attribution_touchpoint.bizible_form_url,
        mart_crm_attribution_touchpoint.bizible_landing_page,
        mart_crm_attribution_touchpoint.bizible_referrer_page,
        mart_crm_attribution_touchpoint.bizible_marketing_channel,
        case
            when
                mart_crm_attribution_touchpoint.dim_parent_campaign_id
                = '7014M000001dn8MQAQ'
            then 'Paid Social.LinkedIn Lead Gen'
            when
                mart_crm_attribution_touchpoint.bizible_ad_campaign_name
                = '20201013_ActualTechMedia_DeepMonitoringCI'
            then 'Sponsorship'
            else mart_crm_attribution_touchpoint.bizible_marketing_channel_path
        end as marketing_channel_path,
        mart_crm_attribution_touchpoint.pipe_name,
        mart_crm_attribution_touchpoint.bizible_medium,
        mart_crm_attribution_touchpoint.lead_source,
        mart_crm_attribution_touchpoint.opportunity_created_date::date
        as opp_created_date,
        mart_crm_attribution_touchpoint.sales_accepted_date::date
        as sales_accepted_date,
        mart_crm_attribution_touchpoint.opportunity_close_date::date as close_date,
        mart_crm_attribution_touchpoint.sales_type,
        mart_crm_attribution_touchpoint.stage_name,
        mart_crm_attribution_touchpoint.is_won,
        mart_crm_attribution_touchpoint.is_sao,
        mart_crm_attribution_touchpoint.deal_path_name,
        mart_crm_attribution_touchpoint.order_type,
        mart_crm_attribution_touchpoint.crm_user_sales_segment,
        mart_crm_attribution_touchpoint.crm_user_region,
        date_trunc(
            'month', mart_crm_attribution_touchpoint.bizible_touchpoint_date
        )::date as bizible_touchpoint_date_month_yr,
        mart_crm_attribution_touchpoint.bizible_touchpoint_date::date
        as bizible_touchpoint_date_normalized,
        mart_crm_attribution_touchpoint.type as campaign_type,
        mart_crm_attribution_touchpoint.last_utm_campaign,
        mart_crm_attribution_touchpoint.last_utm_content,
        mart_crm_attribution_touchpoint.bizible_integrated_campaign_grouping,
        mart_crm_attribution_touchpoint.touchpoint_segment,
        mart_crm_attribution_touchpoint.gtm_motion,
        sum(mart_crm_attribution_touchpoint.bizible_count_first_touch) as first_weight,
        sum(mart_crm_attribution_touchpoint.bizible_count_w_shaped) as w_weight,
        sum(mart_crm_attribution_touchpoint.bizible_count_u_shaped) as u_weight,
        sum(
            mart_crm_attribution_touchpoint.bizible_attribution_percent_full_path
        ) as full_weight,
        sum(
            mart_crm_attribution_touchpoint.bizible_count_custom_model
        ) as custom_weight,
        count(
            distinct mart_crm_attribution_touchpoint.dim_crm_opportunity_id
        ) as l_touches,
        (l_touches / count_touches) as l_weight,
        (mart_crm_attribution_touchpoint.net_arr * first_weight) as first_net_arr,
        (mart_crm_attribution_touchpoint.net_arr * w_weight) as w_net_arr,
        (mart_crm_attribution_touchpoint.net_arr * u_weight) as u_net_arr,
        (mart_crm_attribution_touchpoint.net_arr * full_weight) as full_net_arr,
        (mart_crm_attribution_touchpoint.net_arr * custom_weight) as custom_net_arr,
        (mart_crm_attribution_touchpoint.net_arr * l_weight) as linear_net_arr
    from mart_crm_attribution_touchpoint
    left join
        linear_base
        on mart_crm_attribution_touchpoint.dim_crm_opportunity_id
        = linear_base.dim_crm_opportunity_id
    left join
        campaigns_per_opp
        on mart_crm_attribution_touchpoint.dim_crm_opportunity_id
        = campaigns_per_opp.dim_crm_opportunity_id
        {{ dbt_utils.group_by(n=52) }}

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@rkohnke",
        updated_by="@rkohnke",
        created_date="2022-01-25",
        updated_date="2022-03-02",
    )
}}
