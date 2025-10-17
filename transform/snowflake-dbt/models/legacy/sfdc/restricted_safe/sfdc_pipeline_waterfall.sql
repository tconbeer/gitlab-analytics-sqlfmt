with
    date_details as (

        select
            *,
            dense_rank() over (order by first_day_of_fiscal_quarter) as quarter_number
        from {{ ref("date_details") }}
        order by 1 desc

    ),
    sfdc_accounts_xf as (select * from {{ ref("sfdc_accounts_xf") }}),
    sfdc_opportunity_snapshot_history as (

        select * from {{ ref("sfdc_opportunity_snapshot_history") }}

    ),
    sfdc_opportunity_xf as (select * from {{ ref("sfdc_opportunity_xf") }}),
    beginning as (

        select
            d.fiscal_quarter_name_fy as close_qtr,
            d.fiscal_year as fiscal_close_year,
            d.first_day_of_fiscal_quarter,
            coalesce(o.order_type, '3. Growth') as order_type,
            case
                when
                    (
                        a.ultimate_parent_account_segment = 'Unknown'
                        or a.ultimate_parent_account_segment is null
                    )
                    and o.user_segment = 'SMB'
                then 'SMB'
                when
                    (
                        a.ultimate_parent_account_segment = 'Unknown'
                        or a.ultimate_parent_account_segment is null
                    )
                    and o.user_segment = 'Mid-Market'
                then 'Mid-Market'
                when
                    (
                        a.ultimate_parent_account_segment = 'Unknown'
                        or a.ultimate_parent_account_segment is null
                    )
                    and o.user_segment
                    in ('Large', 'US West', 'US East', 'Public Sector''EMEA', 'APAC')
                then 'Large'
                else a.ultimate_parent_account_segment
            end as sales_segment,
            h.stage_name,
            case
                when
                    h.stage_name in (
                        '00-Pre Opportunity',
                        '0-Pending Acceptance',
                        '0-Qualifying',
                        'Developing',
                        '1-Discovery',
                        '2-Developing',
                        '2-Scoping'
                    )
                then 'Pipeline'
                when
                    h.stage_name in (
                        '3-Technical Evaluation',
                        '4-Proposal',
                        '5-Negotiating',
                        '6-Awaiting Signature',
                        '7-Closing'
                    )
                then '3+ Pipeline'
                when h.stage_name in ('8-Closed Lost', 'Closed Lost')
                then 'Lost'
                when h.stage_name in ('Closed Won')
                then 'Closed Won'
                else 'Other'
            end as stage_name_3plus,
            case
                when
                    h.stage_name in (
                        '00-Pre Opportunity',
                        '0-Pending Acceptance',
                        '0-Qualifying',
                        'Developing',
                        '1-Discovery',
                        '2-Developing',
                        '2-Scoping',
                        '3-Technical Evaluation'
                    )
                then 'Pipeline'
                when
                    h.stage_name in (
                        '4-Proposal',
                        '5-Negotiating',
                        '6-Awaiting Signature',
                        '7-Closing'
                    )
                then '4+ Pipeline'
                when h.stage_name in ('8-Closed Lost', 'Closed Lost')
                then 'Lost'
                when h.stage_name in ('Closed Won')
                then 'Closed Won'
                else 'Other'
            end as stage_name_4plus,
            h.opportunity_id,
            case
                when o.account_owner_team_stamped = 'US East'
                then 'US East'
                when o.account_owner_team_stamped = 'US West'
                then 'US West'
                when o.account_owner_team_stamped = 'EMEA'
                then 'EMEA'
                when o.account_owner_team_stamped = 'APAC'
                then 'APAC'
                when o.account_owner_team_stamped = 'Public Sector'
                then 'Public Sector'
                when
                    o.account_owner_team_stamped in (
                        'Commercial',
                        'Commercial - MM',
                        'MM - East',
                        'MM - West',
                        'MM-EMEA',
                        'MM - EMEA',
                        'MM-APAC'
                    )
                then 'MM'
                when
                    o.account_owner_team_stamped
                    in ('SMB', 'SMB - US', 'SMB - International', 'Commercial - SMB')
                then 'SMB'
                else 'Other'
            end as account_owner_team_stamped,
            date(h.created_date) as created_date,
            date(h.close_date) as close_date,
            count(distinct h.opportunity_id) as opps,
            sum(
                case
                    when
                        h.stage_name in ('8-Closed Lost', 'Closed Lost')
                        and h.sales_type = 'Renewal'
                    then h.renewal_acv * -1
                    when h.stage_name in ('Closed Won')
                    then h.forecasted_iacv
                    else 0
                end
            ) as net_iacv,
            sum(h.forecasted_iacv) as forecasted_iacv,
            sum(o.pre_covid_iacv) as pre_covid_iacv
        from sfdc_opportunity_snapshot_history h
        left join sfdc_opportunity_xf o on h.opportunity_id = o.opportunity_id
        left join sfdc_accounts_xf a on h.account_id = a.account_id
        inner join date_details d on h.close_date = d.date_actual
        inner join date_details dd2 on h.date_actual = dd2.date_actual
        where
            dd2.day_of_fiscal_quarter = 1 and d.quarter_number - dd2.quarter_number = 0
            {{ dbt_utils.group_by(n=12) }}

    ),
    ending as (

        select
            d.fiscal_quarter_name_fy as close_qtr,
            d.fiscal_year as fiscal_close_year,
            d.first_day_of_fiscal_quarter,
            coalesce(o.order_type, '3. Growth') as order_type,
            case
                when
                    (
                        a.ultimate_parent_account_segment = 'Unknown'
                        or a.ultimate_parent_account_segment is null
                    )
                    and o.user_segment = 'SMB'
                then 'SMB'
                when
                    (
                        a.ultimate_parent_account_segment = 'Unknown'
                        or a.ultimate_parent_account_segment is null
                    )
                    and o.user_segment = 'Mid-Market'
                then 'Mid-Market'
                when
                    (
                        a.ultimate_parent_account_segment = 'Unknown'
                        or a.ultimate_parent_account_segment is null
                    )
                    and o.user_segment
                    in ('Large', 'US West', 'US East', 'Public Sector''EMEA', 'APAC')
                then 'Large'
                else a.ultimate_parent_account_segment
            end as sales_segment,
            h.stage_name,
            case
                when
                    h.stage_name in (
                        '00-Pre Opportunity',
                        '0-Pending Acceptance',
                        '0-Qualifying',
                        'Developing',
                        '1-Discovery',
                        '2-Developing',
                        '2-Scoping'
                    )
                then 'Pipeline'
                when
                    h.stage_name in (
                        '3-Technical Evaluation',
                        '4-Proposal',
                        '5-Negotiating',
                        '6-Awaiting Signature',
                        '7-Closing'
                    )
                then '3+ Pipeline'
                when h.stage_name in ('8-Closed Lost', 'Closed Lost')
                then 'Lost'
                when h.stage_name in ('Closed Won')
                then 'Closed Won'
                else 'Other'
            end as stage_name_3plus,
            case
                when
                    h.stage_name in (
                        '00-Pre Opportunity',
                        '0-Pending Acceptance',
                        '0-Qualifying',
                        'Developing',
                        '1-Discovery',
                        '2-Developing',
                        '2-Scoping',
                        '3-Technical Evaluation'
                    )
                then 'Pipeline'
                when
                    h.stage_name in (
                        '4-Proposal',
                        '5-Negotiating',
                        '6-Awaiting Signature',
                        '7-Closing'
                    )
                then '4+ Pipeline'
                when h.stage_name in ('8-Closed Lost', 'Closed Lost')
                then 'Lost'
                when h.stage_name in ('Closed Won')
                then 'Closed Won'
                else 'Other'
            end as stage_name_4plus,
            h.opportunity_id,
            case
                when o.account_owner_team_stamped = 'US East'
                then 'US East'
                when o.account_owner_team_stamped = 'US West'
                then 'US West'
                when o.account_owner_team_stamped = 'EMEA'
                then 'EMEA'
                when o.account_owner_team_stamped = 'APAC'
                then 'APAC'
                when o.account_owner_team_stamped = 'Public Sector'
                then 'Public Sector'
                when
                    o.account_owner_team_stamped in (
                        'Commercial',
                        'Commercial - MM',
                        'MM - East',
                        'MM - West',
                        'MM-EMEA',
                        'MM - EMEA',
                        'MM-APAC'
                    )
                then 'MM'
                when
                    o.account_owner_team_stamped
                    in ('SMB', 'SMB - US', 'SMB - International', 'Commercial - SMB')
                then 'SMB'
                else 'Other'
            end as account_owner_team_stamped,
            date(h.created_date) as created_date,
            date(h.close_date) as close_date,
            count(distinct h.opportunity_id) as opps,
            sum(
                case
                    when
                        h.stage_name in ('8-Closed Lost', 'Closed Lost')
                        and h.sales_type = 'Renewal'
                    then h.renewal_acv * -1
                    when h.stage_name in ('Closed Won')
                    then h.forecasted_iacv
                    else 0
                end
            ) as net_iacv,
            sum(h.forecasted_iacv) as forecasted_iacv,
            sum(o.pre_covid_iacv) as pre_covid_iacv
        from sfdc_opportunity_snapshot_history h
        left join sfdc_opportunity_xf o on h.opportunity_id = o.opportunity_id
        left join sfdc_accounts_xf a on h.account_id = a.account_id
        inner join date_details d on h.close_date = d.date_actual
        inner join date_details dd2 on h.date_actual = dd2.date_actual
        where
            dd2.day_of_fiscal_quarter = 1 and d.quarter_number - dd2.quarter_number = -1
            {{ dbt_utils.group_by(n=12) }}

    ),
    combined as (

        select
            coalesce(b.opportunity_id, e.opportunity_id) as opportunity_id,
            coalesce(b.close_qtr, e.close_qtr) as close_qtr,
            coalesce(b.fiscal_close_year, e.fiscal_close_year) as fiscal_close_year,
            coalesce(
                b.first_day_of_fiscal_quarter, e.first_day_of_fiscal_quarter
            ) as first_day_of_fiscal_quarter,
            b.order_type,
            e.order_type as order_type_ending,
            b.sales_segment,
            e.sales_segment as sales_segment_ending,
            b.account_owner_team_stamped,
            e.account_owner_team_stamped as account_owner_team_ending,
            b.stage_name,
            e.stage_name as stage_name_ending,
            b.stage_name_3plus,
            e.stage_name_3plus as stage_name_3plus_ending,
            b.stage_name_4plus,
            e.stage_name_4plus as stage_name_4plus_ending,
            b.created_date,
            e.created_date as created_date_ending,
            b.close_date,
            e.close_date as close_date_ending,
            sum(b.opps) as opps,
            sum(e.opps) as opps_ending,
            sum(b.pre_covid_iacv) as c19,
            sum(e.pre_covid_iacv) as c19_ending,
            sum(b.net_iacv) as net_iacv,
            sum(e.net_iacv) as net_iacv_ending,
            sum(b.forecasted_iacv) as forecasted_iacv,
            sum(e.forecasted_iacv) as forecasted_iacv_ending
        from beginning b
        full outer join
            ending e
            on b.opportunity_id || b.close_qtr = e.opportunity_id || e.close_qtr
            {{ dbt_utils.group_by(n=20) }}

    ),
    waterfall as (

        select
            combined.*,
            case
                when close_date is not null then forecasted_iacv else 0
            end as starting_pipeline,
            case
                when
                    created_date_ending >= first_day_of_fiscal_quarter
                    and close_date_ending is not null
                then forecasted_iacv_ending
                else 0
            end as created_in_qtr,
            case
                when
                    created_date_ending < first_day_of_fiscal_quarter
                    and close_date is null
                then forecasted_iacv_ending
                else 0
            end as pulled_in_from_other_qtr,
            case
                when stage_name_ending = '8-Closed Lost' and net_iacv_ending = 0
                then - forecasted_iacv_ending
                else 0
            end as closed_lost,
            case
                when close_date_ending is null then - forecasted_iacv else 0
            end as slipped_deals,
            zeroifnull(- net_iacv_ending) as net_iacv_waterfall,
            case
                when stage_name_ending = 'Closed Won'
                then 0
                when stage_name_ending = '9-Unqualified'
                then 0
                when stage_name_ending = '10-Duplicate'
                then 0
                when stage_name_ending = '8-Closed Lost'
                then 0
                else forecasted_iacv_ending
            end as ending_pipeline,
            case
                when
                    (
                        stage_name_ending = '9-Unqualified'
                        or stage_name_ending = '10-Duplicate'
                    )
                    and close_date_ending is not null
                then - forecasted_iacv_ending
                else 0
            end as duplicate_unqualified

        from combined

    ),
    net_change_in_pipeline_iacv as (

        select
            waterfall.*,
            (
                ending_pipeline - (
                    starting_pipeline
                    + created_in_qtr
                    + pulled_in_from_other_qtr
                    + closed_lost
                    + duplicate_unqualified
                    + slipped_deals
                )
            )
            - net_iacv_waterfall as net_change_in_pipeline_iacv
        from waterfall

    ),
    final as (

        select
            {{ dbt_utils.surrogate_key(["opportunity_id", "close_qtr"]) }}
            as primary_key,
            opportunity_id,
            close_qtr,
            fiscal_close_year,
            first_day_of_fiscal_quarter,
            order_type,
            order_type_ending,
            sales_segment,
            sales_segment_ending,
            account_owner_team_stamped,
            account_owner_team_ending,
            stage_name,
            stage_name_ending,
            stage_name_3plus,
            stage_name_3plus_ending,
            stage_name_4plus,
            stage_name_4plus_ending,
            created_date,
            created_date_ending,
            close_date,
            close_date_ending,
            opps,
            opps_ending,
            c19,
            c19_ending,
            net_iacv,
            net_iacv_ending,
            forecasted_iacv,
            forecasted_iacv_ending,
            starting_pipeline,
            net_change_in_pipeline_iacv,
            created_in_qtr,
            pulled_in_from_other_qtr,
            net_iacv_waterfall,
            closed_lost,
            duplicate_unqualified,
            slipped_deals,
            ending_pipeline
        from net_change_in_pipeline_iacv

    )

select *
from final
where close_date_ending >= '2019-11-01' or close_date >= '2019-11-01'
