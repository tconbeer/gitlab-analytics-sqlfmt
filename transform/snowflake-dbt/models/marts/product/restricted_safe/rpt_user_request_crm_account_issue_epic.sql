with
    mart_user_request as (select * from {{ ref("mart_user_request") }}),
    issue_account_summary as (

        select
            {{
                dbt_utils.surrogate_key(
                    ["dim_issue_id", "dim_epic_id", "dim_crm_account_id"]
                )
            }} as primary_key,
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

            array_agg(distinct nullif(dim_crm_opportunity_id, md5(-1)))
            within group(
                order by nullif(dim_crm_opportunity_id, md5(-1))
            ) as opportunity_id_array,
            array_agg(distinct nullif(dim_ticket_id, -1))
            within group(order by nullif(dim_ticket_id, -1)) as zendesk_ticket_id_array,

            sum(link_retention_score) as account_retention_score,
            sum(link_growth_score) as account_growth_score,
            sum(link_combined_score) as account_combined_score,
            sum(link_priority_score) as account_priority_score,
            account_priority_score
            / nullifzero(issue_epic_weight) as account_weighted_priority_score,
            iff(
                account_weighted_priority_score is null,
                '[Effort is Empty, Input Effort Here](' || issue_epic_url || ')',
                account_weighted_priority_score::text
            ) as account_weighted_priority_score_input

        from mart_user_request {{ dbt_utils.group_by(n=45) }}

    )

    {{
        dbt_audit(
            cte_ref="issue_account_summary",
            created_by="@jpeguero",
            updated_by="@jpeguero",
            created_date="2021-12-15",
            updated_date="2022-01-05",
        )
    }}
