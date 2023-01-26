{%- macro sfdc_opportunity_fields(model_type) %}

with
    first_contact as (

        select
            opportunity_id,  -- opportunity_id
            contact_id as sfdc_contact_id,
            md5(
                cast(coalesce(cast(contact_id as varchar), '') as varchar)
            ) as dim_crm_person_id,
            row_number() over (
                partition by opportunity_id order by created_date asc
            ) as row_num
        from {{ ref("sfdc_opportunity_contact_role_source") }}

    ),
    attribution_touchpoints as (

        select *
        from {{ ref("sfdc_bizible_attribution_touchpoint_source") }}
        where is_deleted = 'FALSE'

    ),
    date_details as (select * from {{ ref("date_details") }}),
    linear_attribution_base
    as (  -- the number of attribution touches a given opp has in total
        -- linear attribution IACV of an opp / all touches (count_touches) for each
        -- opp - weighted by the number of touches in the given bucket
        -- (campaign,channel,etc)
        select
            opportunity_id as dim_crm_opportunity_id,
            count(
                distinct attribution_touchpoints.touchpoint_id
            ) as count_crm_attribution_touchpoints
        from attribution_touchpoints
        group by 1

    ),
    campaigns_per_opp as (

        select
            opportunity_id as dim_crm_opportunity_id,
            count(distinct attribution_touchpoints.campaign_id) as count_campaigns
        from attribution_touchpoints
        group by 1

    ),
    sfdc_opportunity_stage as (

        select *
        from {{ ref("sfdc_opportunity_stage_source") }} {%- if model_type == "live" %}

    {%- elif model_type == "snapshot" %}
    ),
    snapshot_dates as (

        select *
        from {{ ref("dim_date") }}
        where
            date_actual >= '2020-03-01' and date_actual <= current_date
            {% if is_incremental() %}

            -- this filter will only be applied on an incremental run
            and date_id > (select max(snapshot_id) from {{ this }})

            {% endif %}
    {%- endif %}),
    sfdc_opportunity as (

        select
            account_id as dim_crm_account_id,
            opportunity_id as dim_crm_opportunity_id,
            owner_id as dim_crm_user_id,
            order_type_stamped as order_type,
            opportunity_term as opportunity_term_base,
            {{ sales_qualified_source_cleaning("sales_qualified_source") }}
            as sales_qualified_source,
            user_segment_stamped as crm_opp_owner_sales_segment_stamped,
            user_geo_stamped as crm_opp_owner_geo_stamped,
            user_region_stamped as crm_opp_owner_region_stamped,
            user_area_stamped as crm_opp_owner_area_stamped,
            user_segment_geo_region_area_stamped
            as crm_opp_owner_sales_segment_geo_region_area_stamped,
            created_date::date as created_date,
            sales_accepted_date::date as sales_accepted_date,
            close_date::date as close_date,
            {%- if model_type == "live" %}
            {{
                dbt_utils.star(
                    from=ref("sfdc_opportunity_source"),
                    except=[
                        "ACCOUNT_ID",
                        "OPPORTUNITY_ID",
                        "OWNER_ID",
                        "ORDER_TYPE_STAMPED",
                        "IS_WON",
                        "ORDER_TYPE",
                        "OPPORTUNITY_TERM",
                        "SALES_QUALIFIED_SOURCE",
                        "DBT_UPDATED_AT",
                        "CREATED_DATE",
                        "SALES_ACCEPTED_DATE",
                        "CLOSE_DATE",
                    ],
                )
            }}
            {%- elif model_type == "snapshot" %}
            {{
                dbt_utils.surrogate_key(
                    [
                        "sfdc_opportunity_snapshots_source.opportunity_id",
                        "snapshot_dates.date_id",
                    ]
                )
            }} as crm_opportunity_snapshot_id,
            snapshot_dates.date_id as snapshot_id,
            {{
                dbt_utils.star(
                    from=ref("sfdc_opportunity_snapshots_source"),
                    except=[
                        "ACCOUNT_ID",
                        "OPPORTUNITY_ID",
                        "OWNER_ID",
                        "ORDER_TYPE_STAMPED",
                        "IS_WON",
                        "ORDER_TYPE",
                        "OPPORTUNITY_TERM",
                        "SALES_QUALIFIED_SOURCE",
                        "DBT_UPDATED_AT",
                        "CREATED_DATE",
                        "SALES_ACCEPTED_DATE",
                        "CLOSE_DATE",
                    ],
                )
            }}
            {%- endif %}
        from {%- if model_type == "live" %} {{ ref("sfdc_opportunity_source") }}
        {%- elif model_type == "snapshot" %}
            {{ ref("sfdc_opportunity_snapshots_source") }}
        inner join
            snapshot_dates
            on snapshot_dates.date_actual
            >= sfdc_opportunity_snapshots_source.dbt_valid_from
            and snapshot_dates.date_actual < coalesce(
                sfdc_opportunity_snapshots_source.dbt_valid_to, '9999-12-31'::timestamp
            )
        {%- endif %}
        where account_id is not null and is_deleted = false

    ),
    sfdc_zqu_quote_source as (

        select * from {{ ref("sfdc_zqu_quote_source") }} where is_deleted = false

    ),
    quote as (

        select distinct
            sfdc_zqu_quote_source.zqu__opportunity as dim_crm_opportunity_id,
            sfdc_zqu_quote_source.quote_id as dim_quote_id,
            sfdc_zqu_quote_source.zqu__start_date::date as quote_start_date,
            (
                row_number() over (
                    partition by sfdc_zqu_quote_source.zqu__opportunity
                    order by sfdc_zqu_quote_source.created_date desc
                )
            ) as record_number
        from sfdc_zqu_quote_source
        inner join
            sfdc_opportunity
            on sfdc_zqu_quote_source.zqu__opportunity
            = sfdc_opportunity.dim_crm_opportunity_id
        where stage_name in ('Closed Won', '8-Closed Lost') and zqu__primary = true
        qualify record_number = 1

    ),
    sfdc_account as (

        select
            {%- if model_type == "live" %} *
            {%- elif model_type == "snapshot" %}
            {{
                dbt_utils.surrogate_key(
                    [
                        "sfdc_account_snapshots_source.account_id",
                        "snapshot_dates.date_id",
                    ]
                )
            }} as crm_account_snapshot_id,
            snapshot_dates.date_id as snapshot_id,
            sfdc_account_snapshots_source.*
            {%- endif %}
        from {%- if model_type == "live" %} {{ ref("sfdc_account_source") }}
        {%- elif model_type == "snapshot" %}
            {{ ref("sfdc_account_snapshots_source") }}
        inner join
            snapshot_dates
            on snapshot_dates.date_actual
            >= sfdc_account_snapshots_source.dbt_valid_from
            and snapshot_dates.date_actual < coalesce(
                sfdc_account_snapshots_source.dbt_valid_to, '9999-12-31'::timestamp
            )
        {%- endif %}
        where account_id is not null

    ),
    final as (

        select
            -- opportunity information
            sfdc_opportunity.*,
            case
                when
                    (
                        sfdc_opportunity.days_in_stage > 30
                        or sfdc_opportunity.incremental_acv > 100000
                        or sfdc_opportunity.pushed_count > 0
                    )
                then true
                else false
            end as is_risky,
            case
                when sfdc_opportunity.opportunity_term_base is null
                then
                    datediff(
                        'month',
                        quote.quote_start_date,
                        sfdc_opportunity.subscription_end_date
                    )
                else sfdc_opportunity.opportunity_term_base
            end as opportunity_term,
            -- opportunity stage information 
            sfdc_opportunity_stage.is_active as stage_is_active,
            sfdc_opportunity_stage.is_closed as stage_is_closed,
            sfdc_opportunity_stage.is_won as is_won,

            -- flags
            case
                when
                    sfdc_opportunity.sales_accepted_date is not null
                    and sfdc_opportunity.is_edu_oss = 0
                    and sfdc_opportunity.stage_name != '10-Duplicate'
                then true
                else false
            end as is_sao,
            case
                when
                    is_sao = true
                    and sfdc_opportunity.sales_qualified_source
                    in ('SDR Generated', 'BDR Generated')
                then true
                else false
            end as is_sdr_sao,
            case
                when
                    (
                        (
                            sfdc_opportunity.sales_type = 'Renewal'
                            and stage_name = '8-Closed Lost'
                        )
                        or sfdc_opportunity.stage_name = 'Closed Won'
                    )
                    and sfdc_account.is_jihu_account = false
                then true
                else false
            end as is_net_arr_closed_deal,
            case
                when
                    sfdc_opportunity.new_logo_count = 1
                    or sfdc_opportunity.new_logo_count = -1
                then true
                else false
            end as is_new_logo_first_order,
            case
                when
                    sfdc_opportunity.is_edu_oss = 0
                    and sfdc_opportunity.stage_name
                    not in ('00-Pre Opportunity', '10-Duplicate')
                then true
                else false
            end as is_net_arr_pipeline_created,
            case
                when
                    sfdc_opportunity_stage.is_closed = true
                    and sfdc_opportunity.amount >= 0
                    and (
                        sfdc_opportunity.reason_for_loss is null
                        or sfdc_opportunity.reason_for_loss
                        != 'Merged into another opportunity'
                    )
                    and sfdc_opportunity.is_edu_oss = 0
                then true
                else false
            end as is_win_rate_calc,
            case
                when
                    sfdc_opportunity_stage.is_won = 'TRUE'
                    and sfdc_opportunity.is_closed = 'TRUE'
                    and sfdc_opportunity.is_edu_oss = 0
                then true
                else false
            end as is_closed_won,
            case
                when sfdc_opportunity.days_in_sao < 0
                then '1. Closed in < 0 days'
                when sfdc_opportunity.days_in_sao between 0 and 30
                then '2. Closed in 0-30 days'
                when sfdc_opportunity.days_in_sao between 31 and 60
                then '3. Closed in 31-60 days'
                when sfdc_opportunity.days_in_sao between 61 and 90
                then '4. Closed in 61-90 days'
                when sfdc_opportunity.days_in_sao between 91 and 180
                then '5. Closed in 91-180 days'
                when sfdc_opportunity.days_in_sao between 181 and 270
                then '6. Closed in 181-270 days'
                when sfdc_opportunity.days_in_sao > 270
                then '7. Closed in > 270 days'
                else null
            end as closed_buckets,
            case
                when sfdc_opportunity.created_date < '2022-02-01'
                then 'Legacy'
                when
                    sfdc_opportunity.opportunity_sales_development_representative
                    is not null
                    and sfdc_opportunity.opportunity_business_development_representative
                    is not null
                then 'SDR & BDR'
                when
                    sfdc_opportunity.opportunity_sales_development_representative
                    is not null
                then 'SDR'
                when
                    sfdc_opportunity.opportunity_business_development_representative
                    is not null
                then 'BDR'
                when
                    sfdc_opportunity.opportunity_business_development_representative
                    is null
                    and sfdc_opportunity.opportunity_sales_development_representative
                    is null
                then 'No XDR Assigned'
            end as sdr_or_bdr,

            -- alliance type fields
            {{
                alliance_type(
                    "fulfillment_partner.account_name",
                    "sfdc_opportunity.fulfillment_partner",
                )
            }},
            {{
                alliance_type_short(
                    "fulfillment_partner.account_name",
                    "sfdc_opportunity.fulfillment_partner",
                )
            }},

            -- date ids
            {{ get_date_id("sfdc_opportunity.created_date") }} as created_date_id,
            {{ get_date_id("sfdc_opportunity.sales_accepted_date") }}
            as sales_accepted_date_id,
            {{ get_date_id("sfdc_opportunity.close_date") }} as close_date_id,
            {{ get_date_id("sfdc_opportunity.stage_0_pending_acceptance_date") }}
            as stage_0_pending_acceptance_date_id,
            {{ get_date_id("sfdc_opportunity.stage_1_discovery_date") }}
            as stage_1_discovery_date_id,
            {{ get_date_id("sfdc_opportunity.stage_2_scoping_date") }}
            as stage_2_scoping_date_id,
            {{ get_date_id("sfdc_opportunity.stage_3_technical_evaluation_date") }}
            as stage_3_technical_evaluation_date_id,
            {{ get_date_id("sfdc_opportunity.stage_4_proposal_date") }}
            as stage_4_proposal_date_id,
            {{ get_date_id("sfdc_opportunity.stage_5_negotiating_date") }}
            as stage_5_negotiating_date_id,
            {{ get_date_id("sfdc_opportunity.stage_6_closed_won_date") }}
            as stage_6_closed_won_date_id,
            {{ get_date_id("sfdc_opportunity.stage_6_closed_lost_date") }}
            as stage_6_closed_lost_date_id,

            -- quote information
            quote.dim_quote_id,
            quote.quote_start_date,

            -- contact information
            first_contact.dim_crm_person_id,
            first_contact.sfdc_contact_id,

            -- attribution information
            linear_attribution_base.count_crm_attribution_touchpoints,
            campaigns_per_opp.count_campaigns,
            incremental_acv
            / linear_attribution_base.count_crm_attribution_touchpoints
            as weighted_linear_iacv,

            -- Noel's fields
            close_date_detail.first_day_of_month as close_date_month,
            close_date_detail.fiscal_year as close_fiscal_year,
            close_date_detail.fiscal_quarter_name_fy as close_fiscal_quarter_name,
            close_date_detail.first_day_of_fiscal_quarter as close_fiscal_quarter_date,

            created_date_detail.first_day_of_month as created_date_month,
            created_date_detail.fiscal_year as created_fiscal_year,
            created_date_detail.fiscal_quarter_name_fy as created_fiscal_quarter_name,
            created_date_detail.first_day_of_fiscal_quarter
            as created_fiscal_quarter_date,

            net_arr_created_date.first_day_of_month as iacv_created_date_month,
            net_arr_created_date.fiscal_year as iacv_created_fiscal_year,
            net_arr_created_date.fiscal_quarter_name_fy
            as iacv_created_fiscal_quarter_name,
            net_arr_created_date.first_day_of_fiscal_quarter
            as iacv_created_fiscal_quarter_date,

            created_date_detail.date_actual as net_arr_created_date,
            created_date_detail.first_day_of_month as net_arr_created_date_month,
            created_date_detail.fiscal_year as net_arr_created_fiscal_year,
            created_date_detail.fiscal_quarter_name_fy
            as net_arr_created_fiscal_quarter_name,
            created_date_detail.first_day_of_fiscal_quarter
            as net_arr_created_fiscal_quarter_date,

            net_arr_created_date.date_actual as pipeline_created_date,
            net_arr_created_date.first_day_of_month as pipeline_created_date_month,
            net_arr_created_date.fiscal_year as pipeline_created_fiscal_year,
            net_arr_created_date.fiscal_quarter_name_fy
            as pipeline_created_fiscal_quarter_name,
            net_arr_created_date.first_day_of_fiscal_quarter
            as pipeline_created_fiscal_quarter_date,

            sales_accepted_date.first_day_of_month as sales_accepted_month,
            sales_accepted_date.fiscal_year as sales_accepted_fiscal_year,
            sales_accepted_date.fiscal_quarter_name_fy
            as sales_accepted_fiscal_quarter_name,
            sales_accepted_date.first_day_of_fiscal_quarter
            as sales_accepted_fiscal_quarter_date,

            start_date.fiscal_quarter_name_fy
            as subscription_start_date_fiscal_quarter_name,
            start_date.first_day_of_fiscal_quarter
            as subscription_start_date_fiscal_quarter_date,
            start_date.fiscal_year as subscription_start_date_fiscal_year,
            start_date.first_day_of_month as subscription_start_date_month,

            case
                when
                    sfdc_opportunity.stage_name in (
                        '1-Discovery',
                        '2-Developing',
                        '2-Scoping',
                        '3-Technical Evaluation',
                        '4-Proposal',
                        'Closed Won',
                        '5-Negotiating',
                        '6-Awaiting Signature',
                        '7-Closing'
                    )
                then 1
                else 0
            end as is_stage_1_plus,
            case
                when
                    sfdc_opportunity.stage_name in (
                        '3-Technical Evaluation',
                        '4-Proposal',
                        'Closed Won',
                        '5-Negotiating',
                        '6-Awaiting Signature',
                        '7-Closing'
                    )
                then 1
                else 0
            end as is_stage_3_plus,
            case
                when
                    sfdc_opportunity.stage_name in (
                        '4-Proposal',
                        'Closed Won',
                        '5-Negotiating',
                        '6-Awaiting Signature',
                        '7-Closing'
                    )
                then 1
                else 0
            end as is_stage_4_plus,
            case
                when sfdc_opportunity.stage_name in ('8-Closed Lost', 'Closed Lost')
                then 1
                else 0
            end as is_lost,
            case
                when
                    sfdc_opportunity.stage_name in (
                        '8-Closed Lost',
                        'Closed Lost',
                        '9-Unqualified',
                        'Closed Won',
                        '10-Duplicate'
                    )
                then 0
                else 1
            end as is_open,
            case
                when lower(sfdc_opportunity.sales_type) like '%renewal%' then 1 else 0
            end as is_renewal,
            case
                when sfdc_opportunity.opportunity_category in ('Decommission')
                then 1
                else 0
            end as is_decommissed,
            case
                when is_won = 1
                then '1.Won'
                when is_lost = 1
                then '2.Lost'
                when is_open = 1
                then '0. Open'
                else 'N/A'
            end as stage_category,
            case
                when
                    lower(sfdc_opportunity.order_type_grouped)
                    like any ('%growth%', '%new%')
                    and sfdc_opportunity.is_edu_oss = 0
                    and is_stage_1_plus = 1
                    and sfdc_opportunity.forecast_category_name != 'Omitted'
                    and is_open = 1
                then 1
                else 0
            end as is_eligible_open_pipeline,
            case
                when
                    sfdc_opportunity.order_type
                    in ('1. New - First Order', '2. New - Connected', '3. Growth')
                    and sfdc_opportunity.is_edu_oss = 0
                    and pipeline_created_fiscal_quarter_date is not null
                    and sfdc_opportunity.opportunity_category in (
                        'Standard',
                        'Internal Correction',
                        'Ramp Deal',
                        'Credit',
                        'Contract Reset'
                    )
                    and (is_stage_1_plus = 1 or is_lost = 1)
                    and sfdc_opportunity.stage_name
                    not in ('10-Duplicate', '9-Unqualified')
                    and (
                        sfdc_opportunity.net_arr > 0
                        or sfdc_opportunity.opportunity_category = 'Credit'
                    )
                    -- -- exclude vision opps from FY21-Q2
                    -- AND (sfdc_opportunity.pipeline_created_fiscal_quarter_name !=
                    -- 'FY21-Q2'
                    -- OR vision_opps.opportunity_id IS NULL)
                    -- 20220128 Updated to remove webdirect SQS deals 
                    and sfdc_opportunity.sales_qualified_source
                    != 'Web Direct Generated'
                then 1
                else 0
            end as is_eligible_created_pipeline,
            case
                when
                    sfdc_opportunity.sales_accepted_date is not null
                    and sfdc_opportunity.is_edu_oss = 0
                    and sfdc_opportunity.is_deleted = 0
                then 1
                else 0
            end as is_eligible_sao,
            case
                when
                    sfdc_opportunity.is_edu_oss = 0
                    and sfdc_opportunity.is_deleted = 0
                    and sfdc_opportunity.order_type
                    in ('1. New - First Order', '2. New - Connected', '3. Growth')
                    and sfdc_opportunity.opportunity_category
                    in ('Standard', 'Ramp Deal', 'Internal Correction')
                    and (
                        (
                            sfdc_opportunity.is_web_portal_purchase = 1
                            and sfdc_opportunity.net_arr > 0
                        )
                        or sfdc_opportunity.is_web_portal_purchase = 0
                    )
                then 1
                else 0
            end as is_eligible_asp_analysis,
            case
                when
                    sfdc_opportunity.is_edu_oss = 0
                    and sfdc_opportunity.is_deleted = 0
                    and is_renewal = 0
                    and sfdc_opportunity.order_type in (
                        '1. New - First Order',
                        '2. New - Connected',
                        '3. Growth',
                        '4. Contraction',
                        '6. Churn - Final',
                        '5. Churn - Partial'
                    )
                    and sfdc_opportunity.opportunity_category
                    in ('Standard', 'Ramp Deal', 'Decommissioned')
                    and sfdc_opportunity.is_web_portal_purchase = 0
                then 1
                else 0
            end as is_eligible_age_analysis,
            case
                when
                    sfdc_opportunity.is_edu_oss = 0
                    and sfdc_opportunity.is_deleted = 0
                    and (is_won = 1 or (is_renewal = 1 and is_lost = 1))
                    and sfdc_opportunity.order_type in (
                        '1. New - First Order',
                        '2. New - Connected',
                        '3. Growth',
                        '4. Contraction',
                        '6. Churn - Final',
                        '5. Churn - Partial'
                    )
                then 1
                else 0
            end as is_eligible_net_arr,
            case
                when
                    sfdc_opportunity.is_edu_oss = 0
                    and sfdc_opportunity.is_deleted = 0
                    and sfdc_opportunity.order_type
                    in ('4. Contraction', '6. Churn - Final', '5. Churn - Partial')
                then 1
                else 0
            end as is_eligible_churn_contraction,
            case
                when sfdc_opportunity.stage_name in ('10-Duplicate') then 1 else 0
            end as is_duplicate,
            case
                when sfdc_opportunity.opportunity_category in ('Credit') then 1 else 0
            end as is_credit,
            case
                when sfdc_opportunity.opportunity_category in ('Contract Reset')
                then 1
                else 0
            end as is_contract_reset,
            coalesce(
                sfdc_opportunity.reason_for_loss, sfdc_opportunity.downgrade_reason
            ) as reason_for_loss_staged,
            case
                when
                    reason_for_loss_staged
                    in ('Do Nothing', 'Other', 'Competitive Loss', 'Operational Silos')
                    or reason_for_loss_staged is null
                then 'Unknown'
                when
                    reason_for_loss_staged in (
                        'Missing Feature',
                        'Product value/gaps',
                        'Product Value / Gaps',
                        'Stayed with Community Edition',
                        'Budget/Value Unperceived'
                    )
                then 'Product Value / Gaps'
                when
                    reason_for_loss_staged in (
                        'Lack of Engagement / Sponsor', 'Went Silent', 'Evangelist Left'
                    )
                then 'Lack of Engagement / Sponsor'
                when reason_for_loss_staged in ('Loss of Budget', 'No budget')
                then 'Loss of Budget'
                when reason_for_loss_staged = 'Merged into another opportunity'
                then 'Merged Opp'
                when reason_for_loss_staged = 'Stale Opportunity'
                then 'No Progression - Auto-close'
                when
                    reason_for_loss_staged in (
                        'Product Quality / Availability', 'Product quality/availability'
                    )
                then 'Product Quality / Availability'
                else reason_for_loss_staged
            end as reason_for_loss_calc,
            case
                when
                    sfdc_opportunity.order_type
                    in ('4. Contraction', '5. Churn - Partial')
                then 'Contraction'
                else 'Churn'
            end as churn_contraction_type_calc,
            case
                when
                    is_renewal = 1
                    and subscription_start_date_fiscal_quarter_date
                    >= close_fiscal_quarter_date
                then 'On-Time'
                when
                    is_renewal = 1
                    and subscription_start_date_fiscal_quarter_date
                    < close_fiscal_quarter_date
                then 'Late'
            end as renewal_timing_status,
            case
                when net_arr > -5000
                then '1. < 5k'
                when net_arr > -20000 and net_arr <= -5000
                then '2. 5k-20k'
                when net_arr > -50000 and net_arr <= -20000
                then '3. 20k-50k'
                when net_arr > -100000 and net_arr <= -50000
                then '4. 50k-100k'
                when net_arr < -100000
                then '5. 100k+'
            end as churned_contraction_net_arr_bucket,
            case
                when is_decommissed = 1 then -1 when is_credit = 1 then 0 else 1
            end as calculated_deal_count

        from sfdc_opportunity
        inner join
            sfdc_opportunity_stage
            on sfdc_opportunity.stage_name = sfdc_opportunity_stage.primary_label
        left join
            quote
            on sfdc_opportunity.dim_crm_opportunity_id = quote.dim_crm_opportunity_id
        left join
            linear_attribution_base
            on sfdc_opportunity.dim_crm_opportunity_id
            = linear_attribution_base.dim_crm_opportunity_id
        left join
            campaigns_per_opp
            on sfdc_opportunity.dim_crm_opportunity_id
            = campaigns_per_opp.dim_crm_opportunity_id
        left join
            first_contact
            on sfdc_opportunity.dim_crm_opportunity_id = first_contact.opportunity_id
            and first_contact.row_num = 1
        left join
            date_details as close_date_detail
            on sfdc_opportunity.close_date = close_date_detail.date_actual
        left join
            date_details as created_date_detail
            on sfdc_opportunity.created_date = created_date_detail.date_actual
        left join
            date_details as net_arr_created_date
            on sfdc_opportunity.iacv_created_date::date
            = net_arr_created_date.date_actual
        left join
            date_details as sales_accepted_date
            on sfdc_opportunity.sales_accepted_date = sales_accepted_date.date_actual
        left join
            date_details as start_date
            on sfdc_opportunity.subscription_start_date::date = start_date.date_actual
        left join
            sfdc_account as fulfillment_partner
            on sfdc_opportunity.fulfillment_partner = fulfillment_partner.account_id
            {%- if model_type == "snapshot" %}
            and sfdc_opportunity.snapshot_id = fulfillment_partner.snapshot_id
            {%- endif %}
        left join
            sfdc_account
            on sfdc_opportunity.dim_crm_account_id = sfdc_account.account_id
            {%- if model_type == "snapshot" %}
            and sfdc_opportunity.snapshot_id = sfdc_account.snapshot_id
            {%- endif %}

    )

{%- endmacro %}
