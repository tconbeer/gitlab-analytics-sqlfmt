{{
    simple_cte(
        [
            ("prep_label_links", "prep_label_links"),
            ("prep_labels", "prep_labels"),
            ("bdg_epic_user_request", "bdg_epic_user_request"),
            ("bdg_issue_user_request", "bdg_issue_user_request"),
            ("dim_epic", "dim_epic"),
            ("dim_issue", "dim_issue"),
            ("fct_mrr", "fct_mrr"),
            ("dim_date", "dim_date"),
            ("dim_product_detail", "dim_product_detail"),
            ("dim_crm_account", "dim_crm_account"),
            ("dim_subscription", "dim_subscription"),
            ("fct_crm_opportunity", "fct_crm_opportunity"),
            ("dim_crm_user", "dim_crm_user"),
            ("fct_quote_item", "fct_quote_item"),
            ("dim_quote", "dim_quote"),
            ("dim_crm_opportunity", "dim_crm_opportunity"),
            ("dim_order_type", "dim_order_type"),
        ]
    )
}},
opportunity_seats as (

    select
        dim_crm_opportunity.dim_crm_opportunity_id,
        dim_crm_opportunity.dim_crm_account_id,
        dim_crm_opportunity.stage_name,
        dim_crm_opportunity.stage_is_closed,
        dim_crm_opportunity.order_type,
        sum(fct_quote_item.quantity) as quantity
    from fct_quote_item
    inner join
        dim_crm_opportunity
        on dim_crm_opportunity.dim_crm_opportunity_id
        = fct_quote_item.dim_crm_opportunity_id
    inner join
        fct_crm_opportunity
        on fct_crm_opportunity.dim_crm_opportunity_id
        = dim_crm_opportunity.dim_crm_opportunity_id
    inner join dim_quote on dim_quote.dim_quote_id = fct_quote_item.dim_quote_id
    inner join
        dim_product_detail
        on dim_product_detail.dim_product_detail_id
        = fct_quote_item.dim_product_detail_id
    where
        dim_quote.is_primary_quote = true
        and dim_product_detail.product_tier_name in (
            'Plus',
            'GitHost',
            'Standard',
            'Self-Managed - Starter',
            'Self-Managed - Premium',
            'SaaS - Premium',
            'SaaS - Bronze',
            'Basic',
            'Self-Managed - Ultimate',
            'SaaS - Ultimate'
        )
        and fct_crm_opportunity.close_date >= '2019-02-01'
        {{ dbt_utils.group_by(5) }}

),
account_open_fo_opp_seats as (

    select dim_crm_account_id, sum(quantity) as seats
    from opportunity_seats
    where order_type = '1. New - First Order' and stage_is_closed = false
    group by 1

),
opportunity_net_arr as (

    select
        fct_crm_opportunity.dim_crm_opportunity_id,
        fct_crm_opportunity.dim_crm_account_id,
        dim_crm_opportunity.stage_name,
        dim_crm_opportunity.stage_is_closed,
        dim_order_type.order_type_name,
        fct_crm_opportunity.net_arr,
        fct_crm_opportunity.arr_basis
    from fct_crm_opportunity
    inner join
        dim_order_type
        on dim_order_type.dim_order_type_id = fct_crm_opportunity.dim_order_type_id
    inner join
        dim_crm_opportunity
        on dim_crm_opportunity.dim_crm_opportunity_id
        = fct_crm_opportunity.dim_crm_opportunity_id
    -- Net ARR is only good after 2019-02-01
    where fct_crm_opportunity.close_date >= '2019-02-01'

),
account_lost_opp_arr as (

    select dim_crm_account_id, sum(net_arr) as net_arr
    from opportunity_net_arr
    where
        order_type_name in ('1. New - First Order') and stage_name in ('8-Closed Lost')
    group by 1

),
account_lost_customer_arr as (

    select dim_crm_account_id, sum(arr_basis) as arr_basis
    from opportunity_net_arr
    where order_type_name in ('6. Churn - Final') and stage_name in ('8-Closed Lost')
    group by 1

),
account_open_opp_net_arr as (

    select dim_crm_account_id, sum(net_arr) as net_arr
    from opportunity_net_arr
    where stage_is_closed = false
    group by 1

),
account_open_opp_net_arr_fo as (

    select dim_crm_account_id, sum(net_arr) as net_arr
    from opportunity_net_arr
    where stage_is_closed = false and order_type_name in ('1. New - First Order')
    group by 1

),
account_open_opp_net_arr_growth as (

    select dim_crm_account_id, sum(net_arr) as net_arr
    from opportunity_net_arr
    where
        stage_is_closed = false
        and order_type_name in ('2. New - Connected', '3. Growth')
    group by 1

),
account_next_renewal_month as (

    select fct_mrr.dim_crm_account_id, min(subscription_end_month) as next_renewal_month
    from fct_mrr
    inner join dim_date on dim_date.date_id = fct_mrr.dim_date_id
    left join
        dim_subscription
        on dim_subscription.dim_subscription_id = fct_mrr.dim_subscription_id
    where
        dim_subscription.subscription_end_month >= date_trunc('month', current_date)
        and fct_mrr.subscription_status in ('Active', 'Cancelled')
    group by 1

),
arr_metrics_current_month as (

    select
        fct_mrr.dim_crm_account_id,
        sum(fct_mrr.mrr) as mrr,
        sum(fct_mrr.arr) as arr,
        sum(fct_mrr.quantity) as quantity
    from fct_mrr
    inner join dim_date on dim_date.date_id = fct_mrr.dim_date_id
    inner join
        dim_product_detail
        on dim_product_detail.dim_product_detail_id = fct_mrr.dim_product_detail_id
    where
        subscription_status in ('Active', 'Cancelled')
        and dim_date.date_actual = date_trunc('month', current_date)
        and dim_product_detail.product_tier_name in (
            'Plus',
            'GitHost',
            'Standard',
            'Self-Managed - Starter',
            'Self-Managed - Premium',
            'SaaS - Premium',
            'SaaS - Bronze',
            'Basic',
            'Self-Managed - Ultimate',
            'SaaS - Ultimate'
        )
    group by 1

),
epic_weight as (

    select
        dim_epic_id,
        sum(weight) as epic_weight,
        sum(iff(state_name = 'closed', weight, 0))
        / nullifzero(epic_weight) as epic_completeness,
        sum(iff(state_name = 'closed', 1, 0))
        / count(*) as epic_completeness_alternative,
        coalesce(epic_completeness, epic_completeness_alternative) as epic_status
    from dim_issue
    group by 1

),
label_links_joined as (

    select prep_label_links.*, prep_labels.label_title
    from prep_label_links
    left join prep_labels on prep_label_links.dim_label_id = prep_labels.dim_label_id

),
issue_labels as (

    select
        label_links_joined.dim_issue_id,
        iff(
            lower(label_links_joined.label_title) like 'group::%',
            split_part(label_links_joined.label_title, '::', 2),
            null
        ) as group_label,
        iff(
            lower(label_links_joined.label_title) like 'devops::%',
            split_part(label_links_joined.label_title, '::', 2),
            null
        ) as devops_label,
        iff(
            lower(label_links_joined.label_title) like 'section::%',
            split_part(label_links_joined.label_title, '::', 2),
            null
        ) as section_label,
        coalesce(group_label, devops_label, section_label) as product_group_extended,

        iff(
            lower(label_links_joined.label_title) like 'category:%',
            split_part(label_links_joined.label_title, ':', 2),
            null
        ) as category_label,
        iff(
            lower(label_links_joined.label_title) like 'type::%',
            split_part(label_links_joined.label_title, '::', 2),
            null
        ) as type_label,
        case
            when group_label is not null
            then 3
            when devops_label is not null
            then 2
            when section_label is not null
            then 1
            else 0
        end product_group_level
    from label_links_joined

),
epic_labels as (

    select
        label_links_joined.dim_epic_id,
        iff(
            lower(label_links_joined.label_title) like 'group::%',
            split_part(label_links_joined.label_title, '::', 2),
            null
        ) as group_label,
        iff(
            lower(label_links_joined.label_title) like 'devops::%',
            split_part(label_links_joined.label_title, '::', 2),
            null
        ) as devops_label,
        iff(
            lower(label_links_joined.label_title) like 'section::%',
            split_part(label_links_joined.label_title, '::', 2),
            null
        ) as section_label,
        coalesce(group_label, devops_label, section_label) as product_group_extended,

        iff(
            lower(label_links_joined.label_title) like 'category:%',
            split_part(label_links_joined.label_title, ':', 2),
            null
        ) as category_label,
        iff(
            lower(label_links_joined.label_title) like 'type::%',
            split_part(label_links_joined.label_title, '::', 2),
            null
        ) as type_label,
        case
            when group_label is not null
            then 3
            when devops_label is not null
            then 2
            when section_label is not null
            then 1
            else 0
        end product_group_level
    from label_links_joined

-- There is a bug in the product where some scoped labels are used twice. This is a
-- temporary fix for that for the group::* label
),
issue_group_label as (

    select dim_issue_id, group_label
    from issue_labels
    where group_label is not null
    qualify row_number() over (partition by dim_issue_id order by group_label) = 1

),
issue_group_extended_label as (

    select dim_issue_id, product_group_extended
    from issue_labels
    where product_group_extended is not null
    qualify
        row_number() over (partition by dim_issue_id order by product_group_level desc)
        = 1

-- Since category: is not an scoped label, need to make sure I only pull one of them
),
issue_category_dedup as (

    select dim_issue_id, category_label
    from issue_labels
    where category_label is not null
    qualify
        row_number() over (partition by dim_issue_id order by category_label desc) = 1

-- There is a bug in the product where some scoped labels are used twice. This is a
-- temporary fix for that for the type::* label
),
issue_type_label as (

    select dim_issue_id, type_label
    from issue_labels
    where type_label is not null
    qualify row_number() over (partition by dim_issue_id order by type_label) = 1

-- There is a bug in the product where some scoped labels are used twice. This is a
-- temporary fix for that for the devops::* label
),
issue_devops_label as (

    select dim_issue_id, devops_label
    from issue_labels
    where devops_label is not null
    qualify row_number() over (partition by dim_issue_id order by devops_label) = 1

),  -- Some issues for some reason had two valid workflow labels, this dedup them
issue_status as (

    select
        label_links_joined.dim_issue_id,
        iff(
            lower(label_links_joined.label_title) like 'workflow::%',
            split_part(label_links_joined.label_title, '::', 2),
            null
        ) as workflow_label
    from label_links_joined
    where workflow_label is not null
    qualify
        row_number() over (partition by dim_issue_id order by workflow_label desc) = 1

-- There is a bug in the product where some scoped labels are used twice. This is a
-- temporary fix for that for the group::* label
),
epic_group_label as (

    select dim_epic_id, group_label
    from epic_labels
    where group_label is not null
    qualify row_number() over (partition by dim_epic_id order by group_label) = 1

),
epic_group_extended_label as (

    select dim_epic_id, product_group_extended
    from epic_labels
    where product_group_extended is not null
    qualify
        row_number() over (partition by dim_epic_id order by product_group_level desc)
        = 1

-- Since category: is not an scoped label, need to make sure I only pull one of them
),
epic_category_dedup as (

    select dim_epic_id, category_label
    from epic_labels
    where category_label is not null
    qualify
        row_number() over (partition by dim_epic_id order by category_label desc) = 1

-- There is a bug in the product where some scoped labels are used twice. This is a
-- temporary fix for that for the type::* label
),
epic_type_label as (

    select dim_epic_id, type_label
    from epic_labels
    where type_label is not null
    qualify row_number() over (partition by dim_epic_id order by type_label) = 1

-- There is a bug in the product where some scoped labels are used twice. This is a
-- temporary fix for that for the devops::* label
),
epic_devops_label as (

    select dim_epic_id, devops_label
    from epic_labels
    where devops_label is not null
    qualify row_number() over (partition by dim_epic_id order by devops_label) = 1

),  -- Get issue milestone with the latest due dates for epics
epic_last_milestone as (

    select dim_epic_id, milestone_title, milestone_due_date
    from dim_issue
    qualify
        row_number() over (
            partition by dim_epic_id order by milestone_due_date desc nulls last
        )
        = 1

),
user_request as (

    select
        bdg_issue_user_request.dim_issue_id as dim_issue_id,
        ifnull(dim_issue.dim_epic_id, -1) as dim_epic_id,
        'Issue' as user_request_in,

        bdg_issue_user_request.link_type as link_type,
        bdg_issue_user_request.dim_crm_opportunity_id as dim_crm_opportunity_id,
        bdg_issue_user_request.dim_crm_account_id as dim_crm_account_id,
        bdg_issue_user_request.dim_ticket_id as dim_ticket_id,
        bdg_issue_user_request.request_priority as request_priority,
        bdg_issue_user_request.is_request_priority_empty as is_request_priority_empty,
        bdg_issue_user_request.is_user_request_only_in_collaboration_project
        as is_user_request_only_in_collaboration_project,
        bdg_issue_user_request.link_last_updated_at as link_last_updated_at,
        bdg_issue_user_request.link_last_updated_at::date as link_last_updated_date,
        date_trunc(
            'month', bdg_issue_user_request.link_last_updated_at::date
        ) as link_last_updated_month,

        iff(
            link_type = 'Opportunity',
            'https://gitlab.my.salesforce.com/'
            || bdg_issue_user_request.dim_crm_opportunity_id,
            'No Link'
        ) as crm_opportunity_link,
        'https://gitlab.my.salesforce.com/'
        || bdg_issue_user_request.dim_crm_account_id as crm_account_link,
        iff(
            link_type = 'Zendesk Ticket',
            'https://gitlab.zendesk.com/agent/tickets/'
            || bdg_issue_user_request.dim_ticket_id,
            'No Link'
        ) as ticket_link,

        -- Epic / Issue attributes
        dim_issue.issue_title as issue_epic_title,
        dim_issue.issue_url as issue_epic_url,
        dim_issue.created_at as issue_epic_created_at,
        dim_issue.created_at::date as issue_epic_created_date,
        date_trunc('month', dim_issue.created_at::date) as issue_epic_created_month,
        dim_issue.state_name as issue_epic_state_name,
        dim_issue.issue_closed_at as issue_epic_closed_at,
        dim_issue.issue_closed_at::date as issue_epic_closed_date,
        date_trunc('month', dim_issue.issue_closed_at::date) as issue_epic_closed_month,
        dim_issue.milestone_title as milestone_title,
        dim_issue.milestone_due_date as milestone_due_date,
        dim_issue.labels as issue_epic_labels,
        case
            when array_contains('deliverable'::variant, dim_issue.labels)
            then 'Yes'
            when array_contains('stretch'::variant, dim_issue.labels)
            then 'Stretch'
            else 'No'
        end as deliverable,
        ifnull(
            issue_group_extended_label.product_group_extended, 'Unknown'
        ) as product_group_extended,
        group_label.group_label as product_group,
        category_label.category_label as product_category,
        devops_label.devops_label as product_stage,
        case
            type_label.type_label
            when 'bug'
            then 'bug fix'
            when 'feature'
            then 'feature request'
        end as issue_epic_type,
        ifnull(issue_status.workflow_label, 'Not Started') as issue_status,
        -1 as epic_status,
        dim_epic.epic_url as parent_epic_path,
        dim_epic.epic_title as parent_epic_title,
        dim_issue.upvote_count as upvote_count,
        ifnull(dim_issue.weight, 0) as issue_epic_weight

    from bdg_issue_user_request
    left join dim_issue on dim_issue.dim_issue_id = bdg_issue_user_request.dim_issue_id
    left join
        issue_group_extended_label
        on issue_group_extended_label.dim_issue_id = bdg_issue_user_request.dim_issue_id
    left join
        issue_status on issue_status.dim_issue_id = bdg_issue_user_request.dim_issue_id
    left join dim_epic on dim_epic.dim_epic_id = dim_issue.dim_epic_id
    left join
        issue_category_dedup as category_label
        on category_label.dim_issue_id = bdg_issue_user_request.dim_issue_id
    left join
        issue_group_label as group_label
        on group_label.dim_issue_id = bdg_issue_user_request.dim_issue_id
    left join
        issue_devops_label as devops_label
        on devops_label.dim_issue_id = bdg_issue_user_request.dim_issue_id
    left join
        issue_type_label as type_label
        on type_label.dim_issue_id = bdg_issue_user_request.dim_issue_id

    union

    select
        -1 as dim_issue_id,
        bdg_epic_user_request.dim_epic_id as dim_epic_id,
        'Epic' as user_request_in,

        bdg_epic_user_request.link_type as link_type,
        bdg_epic_user_request.dim_crm_opportunity_id as dim_crm_opportunity_id,
        bdg_epic_user_request.dim_crm_account_id as dim_crm_account_id,
        bdg_epic_user_request.dim_ticket_id as dim_ticket_id,
        bdg_epic_user_request.request_priority as request_priority,
        bdg_epic_user_request.is_request_priority_empty as is_request_priority_empty,
        bdg_epic_user_request.is_user_request_only_in_collaboration_project
        as is_user_request_only_in_collaboration_project,
        bdg_epic_user_request.link_last_updated_at as link_last_updated_at,
        bdg_epic_user_request.link_last_updated_at::date as link_last_updated_date,
        date_trunc(
            'month', bdg_epic_user_request.link_last_updated_at::date
        ) as link_last_updated_month,

        iff(
            link_type = 'Opportunity',
            'https://gitlab.my.salesforce.com/'
            || bdg_epic_user_request.dim_crm_opportunity_id,
            'No Link'
        ) as crm_opportunity_link,
        'https://gitlab.my.salesforce.com/'
        || bdg_epic_user_request.dim_crm_account_id as crm_account_link,
        iff(
            link_type = 'Zendesk Ticket',
            'https://gitlab.zendesk.com/agent/tickets/'
            || bdg_epic_user_request.dim_ticket_id,
            'No Link'
        ) as ticket_link,

        -- Epic / Issue attributes
        dim_epic.epic_title as epic_title,
        dim_epic.epic_url as epic_url,
        dim_epic.created_at as issue_epic_created_at,
        dim_epic.created_at::date as issue_epic_created_date,
        date_trunc('month', dim_epic.created_at::date) as issue_epic_created_month,
        dim_epic.state_name as issue_epic_state_name,
        dim_epic.closed_at as issue_epic_closed_at,
        dim_epic.closed_at::date as issue_epic_closed_date,
        date_trunc('month', dim_epic.closed_at::date) as issue_epic_closed_month,
        epic_last_milestone.milestone_title as milestone_title,
        epic_last_milestone.milestone_due_date as milestone_due_date,
        dim_epic.labels as issue_epic_labels,
        case
            when array_contains('deliverable'::variant, dim_epic.labels)
            then 'Yes'
            when array_contains('stretch'::variant, dim_epic.labels)
            then 'Stretch'
            else 'No'
        end as deliverable,
        ifnull(
            epic_group_extended_label.product_group_extended, 'Unknown'
        ) as product_group_extended,
        group_label.group_label as product_group,
        category_label.category_label as product_category,
        devops_label.devops_label as product_stage,
        case
            type_label.type_label
            when 'bug'
            then 'bug fix'
            when 'feature'
            then 'feature request'
        end as issue_epic_type,
        'Not Applicable' as issue_status,
        ifnull(epic_weight.epic_status, 0) as epic_status,
        parent_epic.epic_url as parent_epic_path,
        parent_epic.epic_title as parent_epic_title,
        dim_epic.upvote_count as upvote_count,
        ifnull(epic_weight.epic_weight, 0) as issue_epic_weight

    from bdg_epic_user_request
    left join dim_issue on dim_issue.dim_epic_id = bdg_epic_user_request.dim_epic_id
    left join dim_epic on dim_epic.dim_epic_id = bdg_epic_user_request.dim_epic_id
    left join
        epic_last_milestone
        on epic_last_milestone.dim_epic_id = bdg_epic_user_request.dim_epic_id
    left join
        epic_group_extended_label
        on epic_group_extended_label.dim_epic_id = bdg_epic_user_request.dim_epic_id
    left join epic_weight on epic_weight.dim_epic_id = bdg_epic_user_request.dim_epic_id
    left join dim_epic as parent_epic on parent_epic.dim_epic_id = dim_epic.parent_id
    left join
        epic_category_dedup as category_label
        on category_label.dim_epic_id = bdg_epic_user_request.dim_epic_id
    left join
        epic_group_label as group_label
        on group_label.dim_epic_id = bdg_epic_user_request.dim_epic_id
    left join
        epic_devops_label as devops_label
        on devops_label.dim_epic_id = bdg_epic_user_request.dim_epic_id
    left join
        epic_type_label as type_label
        on type_label.dim_epic_id = bdg_epic_user_request.dim_epic_id

),
user_request_with_account_opp_attributes as (

    select
        {{
            dbt_utils.surrogate_key(
                [
                    "user_request.dim_issue_id",
                    "user_request.dim_epic_id",
                    "user_request.dim_crm_account_id",
                    "user_request.dim_crm_opportunity_id",
                    "user_request.dim_ticket_id",
                ]
            )
        }} as primary_key,
        user_request.*,

        -- CRM Account attributes
        dim_crm_account.crm_account_name as crm_account_name,
        account_next_renewal_month.next_renewal_month as crm_account_next_renewal_month,
        dim_crm_account.health_score_color as crm_account_health_score_color,
        dim_crm_account.parent_crm_account_sales_segment
        as parent_crm_account_sales_segment,
        dim_crm_account.technical_account_manager as technical_account_manager,
        dim_crm_account.crm_account_owner_team as crm_account_owner_team,
        dim_crm_account.account_owner as strategic_account_leader,
        ifnull(arr_metrics_current_month.quantity, 0) as customer_reach,
        ifnull(arr_metrics_current_month.arr, 0) as crm_account_arr,
        ifnull(account_open_opp_net_arr.net_arr, 0) as crm_account_open_opp_net_arr,
        ifnull(
            account_open_opp_net_arr_fo.net_arr, 0
        ) as crm_account_open_opp_net_arr_fo,
        ifnull(
            account_open_opp_net_arr_growth.net_arr, 0
        ) as crm_account_open_opp_net_arr_growth,
        ifnull(account_open_fo_opp_seats.seats, 0) as opportunity_reach,
        ifnull(account_lost_opp_arr.net_arr, 0) as crm_account_lost_opp_net_arr,
        ifnull(account_lost_customer_arr.arr_basis, 0) as crm_account_lost_customer_arr,
        crm_account_lost_opp_net_arr + crm_account_lost_customer_arr as lost_arr,

        -- CRM Opportunity attributes
        dim_crm_opportunity.stage_name as crm_opp_stage_name,
        dim_crm_opportunity.stage_is_closed as crm_opp_is_closed,
        fct_crm_opportunity.close_date as crm_opp_close_date,
        dim_order_type.order_type_name as crm_opp_order_type,
        dim_order_type.order_type_grouped as crm_opp_order_type_grouped,
        iff(
            date_trunc('month', fct_crm_opportunity.subscription_end_date)
            >= date_trunc('month', current_date),
            date_trunc('month', fct_crm_opportunity.subscription_end_date),
            null
        ) as crm_opp_next_renewal_month,
        fct_crm_opportunity.net_arr as crm_opp_net_arr,
        fct_crm_opportunity.arr_basis as crm_opp_arr_basis,
        opportunity_seats.quantity as crm_opp_seats,
        fct_crm_opportunity.probability as crm_opp_probability

    from user_request

    -- Account Joins
    left join
        arr_metrics_current_month
        on arr_metrics_current_month.dim_crm_account_id
        = user_request.dim_crm_account_id
    left join
        dim_crm_account
        on dim_crm_account.dim_crm_account_id = user_request.dim_crm_account_id
    left join
        account_next_renewal_month
        on account_next_renewal_month.dim_crm_account_id
        = user_request.dim_crm_account_id
    left join
        account_open_fo_opp_seats
        on account_open_fo_opp_seats.dim_crm_account_id
        = user_request.dim_crm_account_id
    left join
        account_lost_opp_arr
        on account_lost_opp_arr.dim_crm_account_id = user_request.dim_crm_account_id
    left join
        account_lost_customer_arr
        on account_lost_customer_arr.dim_crm_account_id
        = user_request.dim_crm_account_id
    left join
        account_open_opp_net_arr
        on account_open_opp_net_arr.dim_crm_account_id = user_request.dim_crm_account_id
    left join
        account_open_opp_net_arr_fo
        on account_open_opp_net_arr_fo.dim_crm_account_id
        = user_request.dim_crm_account_id
    left join
        account_open_opp_net_arr_growth
        on account_open_opp_net_arr_growth.dim_crm_account_id
        = user_request.dim_crm_account_id

    -- Opportunity Joins
    left join
        fct_crm_opportunity
        on fct_crm_opportunity.dim_crm_opportunity_id
        = user_request.dim_crm_opportunity_id
    left join
        dim_order_type
        on dim_order_type.dim_order_type_id = fct_crm_opportunity.dim_order_type_id
    left join
        dim_crm_opportunity
        on dim_crm_opportunity.dim_crm_opportunity_id
        = user_request.dim_crm_opportunity_id
    left join
        opportunity_seats
        on opportunity_seats.dim_crm_opportunity_id
        = user_request.dim_crm_opportunity_id

),
customer_value_scores as (

    select
        primary_key,
        case
            when crm_account_health_score_color = 'Green'
            then 1
            when crm_account_health_score_color = 'Yellow'
            then
                case
                    when
                        datediff('months', current_date, crm_account_next_renewal_month)
                        > 18
                    then 1.5
                    when
                        datediff('months', current_date, crm_account_next_renewal_month)
                        > 12
                    then 2
                    when
                        datediff('months', current_date, crm_account_next_renewal_month)
                        <= 12
                    then 2.5
                end
            when crm_account_health_score_color = 'Red'
            then
                case
                    when
                        datediff('months', current_date, crm_account_next_renewal_month)
                        > 18
                    then 2
                    when
                        datediff('months', current_date, crm_account_next_renewal_month)
                        > 12
                    then 3
                    when
                        datediff('months', current_date, crm_account_next_renewal_month)
                        <= 12
                    then 4
                end
            else 1
        end as retention_urgency_score,
        case
            when crm_opp_probability > 60
            then 1
            when crm_opp_probability > 39
            then
                case
                    when datediff('months', current_date, crm_opp_close_date) > 6
                    then 1.25
                    when datediff('months', current_date, crm_opp_close_date) > 3
                    then 1.5
                    when datediff('months', current_date, crm_opp_close_date) <= 3
                    then 2
                end
            when crm_opp_probability < 40
            then
                case
                    when datediff('months', current_date, crm_opp_close_date) > 6
                    then 1.5
                    when datediff('months', current_date, crm_opp_close_date) > 3
                    then 2
                    when datediff('months', current_date, crm_opp_close_date) <= 3
                    then 2.5
                end
            else 1
        end as opportunity_urgency_score,
        iff(
            link_type = 'Opportunity', crm_opp_arr_basis, crm_account_arr
        ) as arr_to_use,
        zeroifnull(
            crm_opp_net_arr
            / nullif(zeroifnull(crm_opp_net_arr) + zeroifnull(arr_to_use), 0)
        ) as growth_percentage,
        zeroifnull(
            arr_to_use / nullif(zeroifnull(crm_opp_net_arr) + zeroifnull(arr_to_use), 0)
        ) as retention_percentage,
        request_priority * growth_percentage as growth_priority,
        request_priority * retention_percentage as retention_priority,
        -- for that account's links in that opportunity - use multiple partitions
        zeroifnull(
            growth_priority / nullif(
                sum(growth_priority) over (
                    partition by dim_crm_account_id, dim_crm_opportunity_id
                ),
                0
            )
        ) as growth_priority_weighting,
        zeroifnull(
            retention_priority
            / nullif(sum(retention_priority) over (partition by dim_crm_account_id), 0)
        ) as retention_priority_weighting,
        -- a utility column to allow sum of all epics for customer reach
        customer_reach / nullif(
            count(*) over (partition by dim_epic_id, dim_crm_account_id), 0
        ) as customer_epic_reach,
        case
            when link_type = 'Opportunity'
            then crm_opp_net_arr * growth_priority_weighting
            else 0
        end as growth_score,
        retention_priority_weighting * crm_account_arr as retention_score,
        growth_score + retention_score as combined_score,
        combined_score * case
            when link_type = 'Opportunity'
            then opportunity_urgency_score
            else retention_urgency_score
        end as priority_score
    from user_request_with_account_opp_attributes
    where
        issue_epic_state_name = 'opened'
        and (
            case
                when link_type = 'Opportunity' then crm_opp_is_closed = false else true
            end
        )

),
final as (

    select
        user_request_with_account_opp_attributes.*,
        case
            when user_request_with_account_opp_attributes.is_request_priority_empty
            then
                '[Input (Using 1 as Default)]('
                || user_request_with_account_opp_attributes.issue_epic_url
                || ')'
            else request_priority::text
        end as priority_input_url,
        case
            when user_request_with_account_opp_attributes.link_type = 'Zendesk Ticket'
            then
                '['
                || user_request_with_account_opp_attributes.link_type
                || ']('
                || user_request_with_account_opp_attributes.ticket_link
                || ')'
            when user_request_with_account_opp_attributes.link_type = 'Opportunity'
            then
                '['
                || user_request_with_account_opp_attributes.link_type
                || ']('
                || user_request_with_account_opp_attributes.crm_opportunity_link
                || ')'
            when user_request_with_account_opp_attributes.link_type = 'Account'
            then
                '['
                || user_request_with_account_opp_attributes.link_type
                || ']('
                || user_request_with_account_opp_attributes.crm_account_link
                || ')'
        end as user_request_link,
        customer_value_scores.retention_percentage as link_retention_percentage,
        customer_value_scores.growth_percentage as link_growth_percentage,
        customer_value_scores.retention_priority as link_retention_priority,
        customer_value_scores.growth_priority as link_growth_priority,
        customer_value_scores.retention_priority_weighting
        as link_retention_priority_weighting,
        customer_value_scores.growth_priority_weighting
        as link_growth_priority_weighting,
        customer_value_scores.retention_score as link_retention_score,
        customer_value_scores.growth_score as link_growth_score,
        customer_value_scores.combined_score as link_combined_score,
        customer_value_scores.priority_score as link_priority_score,
        link_priority_score
        / nullifzero(issue_epic_weight) as link_weighted_priority_score,
        iff(
            link_weighted_priority_score is null,
            '[Effort is Empty, Input Effort Here]('
            || user_request_with_account_opp_attributes.issue_epic_url
            || ')',
            link_weighted_priority_score::text
        ) as link_weighted_priority_score_input
    from user_request_with_account_opp_attributes
    left join
        customer_value_scores
        on user_request_with_account_opp_attributes.primary_key
        = customer_value_scores.primary_key

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@jpeguero",
        updated_by="@jpeguero",
        created_date="2021-10-22",
        updated_date="2022-01-05",
    )
}}
