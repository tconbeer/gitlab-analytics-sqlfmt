with
    mart_user_request as (select * from {{ ref("mart_user_request") }}),
    issue_account_summary
    as (  -- First we summarise at the issue/epic - crm_account grain

        select
            dim_issue_id,
            dim_epic_id,
            user_request_in,
            dim_crm_account_id,

            issue_epic_title,
            issue_epic_url,
            issue_epic_created_at,
            issue_epic_created_date,
            issue_epic_created_month,
            issue_epic_state_name,
            issue_epic_closed_at,
            issue_epic_closed_date,
            issue_epic_closed_month,
            milestone_title,
            milestone_due_date,
            issue_epic_labels,
            deliverable,
            product_group_extended,
            product_group,
            product_category,
            product_stage,
            issue_epic_type,
            issue_status,
            epic_status,
            parent_epic_path,
            parent_epic_title,
            upvote_count,
            issue_epic_weight,

            crm_account_name,
            crm_account_next_renewal_month,
            crm_account_health_score_color,
            parent_crm_account_sales_segment,
            technical_account_manager,
            crm_account_owner_team,
            strategic_account_leader,
            customer_reach,
            crm_account_arr,
            crm_account_open_opp_net_arr,
            crm_account_open_opp_net_arr_fo,
            crm_account_open_opp_net_arr_growth,
            opportunity_reach,
            crm_account_lost_opp_net_arr,
            crm_account_lost_customer_arr,
            lost_arr,

            sum(request_priority) as sum_request_priority,

            sum(crm_opp_net_arr) as sum_linked_crm_opp_net_arr,
            sum(
                iff(crm_opp_is_closed = false, crm_opp_net_arr, 0)
            ) as sum_linked_crm_opp_open_net_arr,
            sum(crm_opp_seats) as sum_linked_crm_opp_seats,
            sum(
                iff(crm_opp_is_closed = false, crm_opp_seats, 0)
            ) as sum_linked_crm_opp_open_seats,

            array_agg(distinct nullif(dim_crm_opportunity_id, md5(-1))) within group (
                order by nullif(dim_crm_opportunity_id, md5(-1))
            ) as opportunity_id_array,
            array_agg(distinct nullif(dim_ticket_id, -1)) within group (
                order by nullif(dim_ticket_id, -1)
            ) as zendesk_ticket_id_array,

            sum(link_retention_score) as account_retention_score,
            sum(link_growth_score) as account_growth_score,
            sum(link_combined_score) as account_combined_score,
            sum(link_priority_score) as account_priority_score

        from mart_user_request {{ dbt_utils.group_by(n=44) }}

    ),
    prep_issue_summary
    as (  -- Then we summarise at the issue/epic grain

        select
            dim_issue_id,
            dim_epic_id,
            user_request_in,

            issue_epic_title,
            issue_epic_url,
            issue_epic_created_at,
            issue_epic_created_date,
            issue_epic_created_month,
            issue_epic_state_name,
            issue_epic_closed_at,
            issue_epic_closed_date,
            issue_epic_closed_month,
            milestone_title,
            milestone_due_date,
            issue_epic_labels,
            deliverable,
            product_group_extended,
            product_group,
            product_category,
            product_stage,
            issue_epic_type,
            issue_status,
            epic_status,
            parent_epic_path,
            parent_epic_title,
            upvote_count,
            issue_epic_weight,

            sum(sum_request_priority) as sum_request_priority,

            -- Account additive fields
            count(distinct dim_crm_account_id) as unique_accounts,
            array_agg(distinct crm_account_name) within group (
                order by crm_account_name
            ) as crm_account_name_array,
            array_agg(distinct crm_account_health_score_color) within group (
                order by crm_account_health_score_color
            ) as crm_account_health_score_color_array,
            array_agg(distinct parent_crm_account_sales_segment) within group (
                order by parent_crm_account_sales_segment
            ) as crm_account_parent_sales_segment_array,
            array_agg(distinct technical_account_manager) within group (
                order by technical_account_manager
            ) as crm_account_tam_array,
            array_agg(distinct crm_account_owner_team) within group (
                order by crm_account_owner_team
            ) as crm_account_owner_team_array,
            array_agg(distinct strategic_account_leader) within group (
                order by strategic_account_leader
            ) as crm_account_strategic_account_leader_array,

            sum(customer_reach) as sum_customer_reach,
            sum(crm_account_arr) as sum_crm_account_arr,
            sum(crm_account_open_opp_net_arr) as sum_crm_account_open_opp_net_arr,
            sum(crm_account_open_opp_net_arr_fo) as sum_crm_account_open_opp_net_arr_fo,
            sum(
                crm_account_open_opp_net_arr_growth
            ) as sum_crm_account_open_opp_net_arr_growth,
            sum(opportunity_reach) as sum_opportunity_reach,
            sum(crm_account_lost_opp_net_arr) as sum_crm_account_lost_opp_net_arr,
            sum(crm_account_lost_customer_arr) as sum_crm_account_lost_customer_arr,
            sum(lost_arr) as sum_lost_arr,

            -- Opportunity additive fields
            sum(sum_linked_crm_opp_net_arr) as sum_linked_crm_opp_net_arr,
            sum(sum_linked_crm_opp_open_net_arr) as sum_linked_crm_opp_open_net_arr,
            sum(sum_linked_crm_opp_seats) as sum_linked_crm_opp_seats,
            sum(sum_linked_crm_opp_open_seats) as sum_linked_crm_opp_open_seats,

            -- Priority score
            sum(account_retention_score) as retention_score,
            sum(account_growth_score) as growth_score,
            sum(account_combined_score) as combined_score,
            sum(account_priority_score) as priority_score,
            priority_score / nullifzero(issue_epic_weight) as weighted_priority_score,
            iff(
                weighted_priority_score is null,
                '[Effort is Empty, Input Effort Here](' || issue_epic_url || ')',
                weighted_priority_score::text
            ) as weighted_priority_score_input

        from issue_account_summary {{ dbt_utils.group_by(n=27) }}

    ),
    prep_issue_opp_zendesk_links as (

        select
            dim_issue_id,
            dim_epic_id,
            count(distinct dim_crm_opportunity_id) as unique_opportunities,
            count(
                distinct iff(crm_opp_is_closed = false, dim_crm_opportunity_id, null)
            ) as unique_open_opportunities,
            array_agg(distinct nullif(dim_crm_opportunity_id, md5(-1))) within group (
                order by nullif(dim_crm_opportunity_id, md5(-1))
            ) as opportunity_id_array,
            array_agg(distinct nullif(dim_ticket_id, -1)) within group (
                order by nullif(dim_ticket_id, -1)
            ) as zendesk_ticket_id_array

        from mart_user_request
        group by 1, 2

    ),
    issue_summary as (

        select
            {{
                dbt_utils.surrogate_key(
                    [
                        "prep_issue_summary.dim_issue_id",
                        "prep_issue_summary.dim_epic_id",
                    ]
                )
            }} as primary_key,
            prep_issue_summary.*,
            prep_issue_opp_zendesk_links.unique_opportunities,
            prep_issue_opp_zendesk_links.unique_open_opportunities,
            prep_issue_opp_zendesk_links.opportunity_id_array,
            prep_issue_opp_zendesk_links.zendesk_ticket_id_array
        from prep_issue_summary
        left join
            prep_issue_opp_zendesk_links
            on prep_issue_opp_zendesk_links.dim_issue_id
            = prep_issue_summary.dim_issue_id
            and prep_issue_opp_zendesk_links.dim_epic_id
            = prep_issue_summary.dim_epic_id
    -- QUALIFY COUNT(*) OVER(PARTITION BY dim_issue_id, dim_epic_id,
    -- dim_crm_account_id) > 1
    )

    {{
        dbt_audit(
            cte_ref="issue_summary",
            created_by="@jpeguero",
            updated_by="@jpeguero",
            created_date="2021-12-15",
            updated_date="2022-01-05",
        )
    }}
